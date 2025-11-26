import logging
import azure.functions as func
import os
import requests
import json
import pandas as pd
import pyodbc
from datetime import datetime

# Inicialização da Function App
app = func.FunctionApp()

# --- VARIÁVEIS DE CONFIGURAÇÃO ---
# Lista de tickers para análise (Ajuste conforme necessário)
TICKERS = ["PETR4", "VALE3", "ITUB4", "BBDC4"] 
FREQUENCY_MINUTES = 30 # Usado para cálculo da MMS

# --- CONEXÃO SQL ---
# A string de conexão é carregada das variáveis de ambiente do Azure
CONNECTION_STRING = os.environ.get('AZURE_SQL_CONNECTION_STRING')
# O token Brapi é carregado das variáveis de ambiente do Azure
BRAPI_TOKEN = os.environ.get('BRAPI_TOKEN')


def calculate_mms_and_alerts(df: pd.DataFrame, ticker: str, period: int) -> tuple:
    """Calcula a Média Móvel Simples (MMS) e gera um alerta se o preço atual
    cruzar a MMS."""

    # 1. Tratamento de dados
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    # Garantir que há dados suficientes
    if len(df) < period:
        logging.warning(f"AVISO: Dados insuficientes para {ticker}. Requer {period} pontos, mas encontrou {len(df)}.")
        return None, None

    # 2. Cálculo da MMS
    df['MMS'] = df['close'].rolling(window=period).mean()

    # 3. Extração dos pontos atuais
    last_row = df.iloc[-1]
    
    # O preço de fechamento atual é o dado que usaremos para comparação
    current_price = last_row['close'] 
    
    # A última MMS calculada
    last_mms = last_row['MMS'] 
    
    # 4. Verificação de Alerta (O preço cruza a MMS de baixo para cima ou de cima para baixo?)
    # Precisamos de pelo menos dois pontos para ver um cruzamento
    if len(df) >= period + 1:
        # Ponto anterior para ver a direção do cruzamento
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
            return alert_type, {
                "ticker": ticker,
                "price": float(current_price),
                "mms": float(last_mms),
                "timestamp": datetime.now().isoformat(),
                "alert_type": alert_type
            }

    return None, None


def insert_alert_into_sql(alert_data: dict):
    """Insere o alerta gerado no Banco de Dados SQL do Azure."""
    
    # Formatação da query SQL
    insert_query = """
    INSERT INTO Alerts (Ticker, Price, MMS, AlertType, Timestamp)
    VALUES (?, ?, ?, ?, ?);
    """
    
    # Conexão e execução
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        
        # Executa a inserção com parâmetros seguros para evitar SQL Injection
        cursor.execute(insert_query, 
                       alert_data['ticker'], 
                       alert_data['price'], 
                       alert_data['mms'], 
                       alert_data['alert_type'], 
                       alert_data['timestamp'])
        
        conn.commit()
        logging.info(f"SUCESSO: Alerta {alert_data['alert_type']} para {alert_data['ticker']} inserido no SQL.")
        return True
        
    except Exception as e:
        logging.error(f"ERRO SQL: Falha ao inserir alerta no banco de dados para {alert_data['ticker']}. Erro: {e}")
        return False
        
    finally:
        if 'conn' in locals() and conn:
            conn.close()


def fetch_data_from_brapi(ticker: str) -> pd.DataFrame:
    """Busca dados de velas do ticker na API Brapi."""
    
    # Parâmetros da API
    # 20 pontos de 30 minutos cobrem 10 horas de negociação (segurança no cálculo)
    interval = "30m"
    limit = 20
    
    # Construção da URL
    url = f"https://brapi.dev/api/v2/finance/candles/{ticker}"
    
    # Parâmetros de requisição
    params = {
        'interval': interval,
        'limit': limit,
        'token': BRAPI_TOKEN
    }
    
    logging.info(f"Buscando dados para {ticker}...")
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status() # Lança exceção para códigos de erro (4xx ou 5xx)
        data = response.json()
        
        # Verifica se os dados da API estão no formato esperado
        if data.get('candles'):
            df = pd.DataFrame(data['candles'])
            
            # Converte timestamps para datetime
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
            df.set_index('datetime', inplace=True)
            
            return df
        else:
            logging.error(f"ERRO BRAAPI: Dados de candles ausentes para {ticker}. Resposta: {data}")
            return pd.DataFrame()
            
    except requests.exceptions.HTTPError as errh:
        logging.error(f"ERRO HTTP para {ticker}: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"ERRO DE CONEXÃO para {ticker}: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"ERRO DE TIMEOUT para {ticker}: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"ERRO GENÉRICO para {ticker}: {err}")
    except Exception as e:
        logging.error(f"ERRO INESPERADO ao buscar dados para {ticker}: {e}")
        
    return pd.DataFrame()


@app.timer_trigger(schedule="0 0 12 * * *", arg_name="myTimer", run_on_startup=False,
                   use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    """Função disparada por tempo para executar a análise de MMS e SQL."""
    
    start_time = datetime.now()
    
    if myTimer.past_due:
        logging.info('O timer estava atrasado.')

    logging.info(f'Iniciando coleta de {len(TICKERS)} ativos na frequência de {FREQUENCY_MINUTES}m...')
    
    alerts_generated = 0

    # 1. Loop através de cada ticker
    for ticker in TICKERS:
        df_candles = fetch_data_from_brapi(ticker)
        
        if not df_candles.empty:
            
            # 2. Calcular MMS e Alertas
            # Usando uma MMS de 14 períodos (comum em análise técnica)
            alert_type, alert_data = calculate_mms_and_alerts(df_candles, ticker, period=14)
            
            # 3. Inserir Alerta no SQL
            if alert_data:
                success = insert_alert_into_sql(alert_data)
                if success:
                    alerts_generated += 1
                
        else:
            logging.warning(f"Ignorando {ticker} devido à falha na busca de dados.")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logging.info(f'--- EXECUÇÃO CONCLUÍDA ---')
    logging.info(f'Total de alertas gerados: {alerts_generated}')
    logging.info(f'Duração total: {duration:.2f} segundos')
    
    if alerts_generated > 0:
        logging.info(f"SUCESSO: {alerts_generated} alerta(s) inserido(s) no Banco de Dados SQL.")
    else:
        logging.info("Nenhum alerta gerado ou inserido nesta execução.")