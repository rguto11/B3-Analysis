import logging
import azure.functions as func
import os
import requests
import json
import pandas as pd
import pyodbc
from datetime import datetime
import time # Importamos 'time' para usar em um mecanismo de backoff simples

# Inicialização da Function App
app = func.FunctionApp()

# --- VARIÁVEIS DE CONFIGURAÇÃO ---
# Lista de tickers para análise
TICKERS = ["PETR4", "VALE3", "ITUB4", "BBDC4"] 
FREQUENCY_MINUTES = 30 # Intervalo da vela para o cálculo
SMA_PERIOD = 14 # Período da Média Móvel Simples (MMS)

# --- CONEXÃO SQL E TOKEN DE API ---
# A string de conexão é carregada das variáveis de ambiente do Azure
CONNECTION_STRING = os.environ.get('AZURE_SQL_CONNECTION_STRING')
# O token Brapi deve ser configurado nas variáveis de ambiente
BRAPI_TOKEN = os.environ.get("BRAPI_TOKEN")


def calculate_mms_and_alerts(df: pd.DataFrame, ticker: str, period: int) -> tuple:
    """
    Calcula a Média Móvel Simples (MMS) e gera um alerta se o preço atual
    cruzar a MMS.
    """

    # 1. Tratamento de dados: Garantir que 'close' é numérico
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    # Garantir que há dados suficientes
    if len(df) < period:
        logging.warning(f"AVISO: Dados insuficientes para {ticker}. Requer {period} pontos, mas encontrou {len(df)}.")
        return None, None

    # 2. Cálculo da MMS
    df['MMS'] = df['close'].rolling(window=period).mean()

    # 3. Extração dos pontos atuais
    last_row = df.iloc[-1]
    
    current_price = last_row['close'] 
    last_mms = last_row['MMS'] 
    
    # 4. Verificação de Alerta (Cruzamento)
    # Precisamos de pelo menos o período + 1 ponto para ver um cruzamento completo
    if len(df) >= period + 1:
        prev_row = df.iloc[-2]
        prev_price = prev_row['close']
        prev_mms = prev_row['MMS']
        
        alert_type = None
        
        # CRUZAMENTO DE COMPRA (Preço sobe e cruza MMS de baixo para cima)
        if prev_price < prev_mms and current_price > last_mms:
            alert_type = "COMPRA"
        
        # CRUZAMENTO DE VENDA (Preço cai e cruza MMS de cima para baixo)
        elif prev_price > prev_mms and current_price < last_mms:
            alert_type = "VENDA"
        
        if alert_type:
            # Garante que os valores numéricos são floats padrão do Python para serialização/SQL
            return alert_type, {
                "ticker": ticker,
                "price": float(current_price),
                "mms": float(last_mms),
                "timestamp": datetime.now().isoformat(),
                "alert_type": alert_type
            }

    return None, None


def insert_alert_into_sql(alert_data: dict, connection_str: str):
    """Insere o alerta gerado no Banco de Dados SQL do Azure usando backoff em caso de falha."""
    
    insert_query = """
    INSERT INTO Alerts (Ticker, Price, MMS, AlertType, Timestamp)
    VALUES (?, ?, ?, ?, ?);
    """
    
    # Implementação de backoff simples para tentar novamente em falhas temporárias de rede/SQL
    max_retries = 3
    
    for attempt in range(max_retries):
        conn = None
        try:
            conn = pyodbc.connect(connection_str)
            cursor = conn.cursor()
            
            cursor.execute(insert_query, 
                           alert_data['ticker'], 
                           alert_data['price'], 
                           alert_data['mms'], 
                           alert_data['alert_type'], 
                           alert_data['timestamp'])
            
            conn.commit()
            logging.info(f"SUCESSO SQL: Alerta {alert_data['alert_type']} para {alert_data['ticker']} inserido.")
            return True # Sucesso
            
        except pyodbc.Error as e:
            logging.error(f"ERRO SQL ({alert_data['ticker']}, tentativa {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logging.info(f"Tentando novamente em {wait_time} segundos...")
                time.sleep(wait_time)
            else:
                logging.error(f"FALHA SQL PERMANENTE: Não foi possível inserir o alerta após {max_retries} tentativas.")
                return False
                
        except Exception as e:
            logging.error(f"ERRO INESPERADO ao inserir alerta para {alert_data['ticker']}: {e}")
            return False
            
        finally:
            if conn:
                conn.close()
    return False


def fetch_data_from_brapi(ticker: str, token: str) -> pd.DataFrame:
    """Busca dados de velas do ticker na API Brapi."""
    
    if not token:
        logging.error("ERRO: BRAPI_TOKEN não está configurado. Não é possível buscar dados.")
        return pd.DataFrame()

    # Intervalo e limite da API (20 pontos de 30 minutos = 10 horas de dados)
    interval = "30m"
    limit = 20
    
    url = f"https://brapi.dev/api/v2/finance/candles/{ticker}"
    
    params = {
        'interval': interval,
        'limit': limit,
        'token': token
    }
    
    logging.info(f"Buscando dados para {ticker}...")
    
    try:
        response = requests.get(url, params=params, timeout=15) # Adicionei um timeout
        response.raise_for_status() 
        data = response.json()
        
        if data.get('candles'):
            df = pd.DataFrame(data['candles'])
            
            # Converte timestamps (segundos) para datetime
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
            df.set_index('datetime', inplace=True)
            
            return df
        else:
            logging.warning(f"AVISO BRAAPI: Dados de candles ausentes para {ticker}. Resposta: {data}")
            return pd.DataFrame()
            
    except requests.exceptions.RequestException as e:
        logging.error(f"ERRO REQUISIÇÃO para {ticker}: {e}")
    except json.JSONDecodeError:
        logging.error(f"ERRO JSON para {ticker}. Resposta: {response.text[:100]}...")
    except Exception as e:
        logging.error(f"ERRO INESPERADO ao buscar dados para {ticker}: {e}")
            
    return pd.DataFrame()


@app.function_name(name="timer_trigger")
# Agenda: A função será executada diariamente ao meio-dia (12:00:00) UTC
# Se precisar de outra frequência, ajuste o cron:
# Ex: "0 */30 * * * *" para a cada 30 minutos
@app.schedule(schedule="0 0 12 * * *", arg_name="myTimer", run_on_startup=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    """Função disparada por tempo para executar a análise de MMS e SQL."""
    
    # Verifica se as dependências críticas estão disponíveis
    if not CONNECTION_STRING or not BRAPI_TOKEN:
        logging.error("ERRO CRÍTICO: AZURE_SQL_CONNECTION_STRING ou BRAPI_TOKEN não estão configurados nas variáveis de ambiente.")
        return

    start_time = datetime.now()
    
    if myTimer.past_due:
        logging.warning('O timer estava atrasado.')

    logging.info(f'Iniciando análise de {len(TICKERS)} ativos. Período MMS: {SMA_PERIOD} velas de {FREQUENCY_MINUTES}m.')
    
    alerts_generated = 0

    # 1. Loop através de cada ticker
    for ticker in TICKERS:
        df_candles = fetch_data_from_brapi(ticker, BRAPI_TOKEN)
        
        if not df_candles.empty:
            
            # 2. Calcular MMS e Alertas
            alert_type, alert_data = calculate_mms_and_alerts(df_candles, ticker, period=SMA_PERIOD)
            
            # 3. Inserir Alerta no SQL
            if alert_data:
                # Passa a string de conexão para a função de inserção
                if insert_alert_into_sql(alert_data, CONNECTION_STRING):
                    alerts_generated += 1
                
        else:
            logging.info(f"Ignorando {ticker} devido à falha ou ausência de dados.")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logging.info(f'--- EXECUÇÃO CONCLUÍDA ---')
    logging.info(f'Total de alertas gerados: {alerts_generated}')
    logging.info(f'Duração total: {duration:.2f} segundos')
    
    if alerts_generated > 0:
        logging.info("SUCESSO: A execução gerou e tentou inserir alertas.")
    else:
        logging.info("Nenhum alerta gerado ou inserido nesta execução.")