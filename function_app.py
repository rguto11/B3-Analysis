import logging
import azure.functions as func
import os
import requests
import json
import pandas as pd
import pyodbc 
from datetime import datetime
import time 

# Inicialização da Function App
app = func.FunctionApp()

# --- VARIÁVEIS DE CONFIGURAÇÃO ---
# Lista de tickers para análise
TICKERS = ["PETR4", "VALE3", "ITUB4", "BBDC4"] 
FREQUENCY_MINUTES = 30 # Intervalo da vela para o cálculo
SMA_PERIOD = 14 # Período da Média Móvel Simples (MMS)
MAX_RETRIES = 3 # Número máximo de tentativas de chamada à API

# --- CONEXÃO SQL E TOKEN DE API ---
# A string de conexão é carregada das variáveis de ambiente do Azure
CONNECTION_STRING = os.environ.get('AZURE_SQL_CONNECTION_STRING')
# O token Brapi deve ser configurado nas variáveis de ambiente.
BRAPI_TOKEN = os.environ.get("BRAPI_TOKEN").strip() if os.environ.get("BRAPI_TOKEN") else None


def calculate_mms_and_alerts(df: pd.DataFrame, ticker: str, period: int) -> tuple:
    """
    Calcula a Média Móvel Simples (MMS) e gera um alerta se o preço atual
    cruzar a MMS.
    """

    # 1. Tratamento de dados: Garantir que 'close' é numérico
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    # Garantir que há dados suficientes para o cálculo do período da MMS
    # Usamos dropna para contar apenas velas com preço válido
    df_valid = df.dropna(subset=['close'])
    if len(df_valid) < period:
        logging.warning(f"AVISO: Dados insuficientes para {ticker}. Requer {period} pontos, mas encontrou {len(df_valid)}.")
        return None, None

    # 2. Cálculo da MMS (no DataFrame com todos os dados, NaN incluídos)
    df['MMS'] = df['close'].rolling(window=period).mean()

    # Filtra apenas linhas onde tanto o preço quanto a MMS são válidos.
    df_analyzable = df.dropna(subset=['close', 'MMS'])
    
    # Precisamos de pelo menos dois pontos válidos (atual e anterior) para detectar o cruzamento.
    if len(df_analyzable) < 2:
        logging.info(f"Dados insuficientes para análise de cruzamento de MMS para {ticker}.")
        return None, None

    # 3. Extração dos pontos atuais e anteriores a partir do DF filtrado
    last_row = df_analyzable.iloc[-1]
    prev_row = df_analyzable.iloc[-2]
    
    current_price = last_row['close']
    last_mms = last_row['MMS']
    
    prev_price = prev_row['close']
    prev_mms = prev_row['MMS']
    
    alert_type = None
    
    # 4. Verificação de Alerta (Cruzamento)
    if prev_price < prev_mms and current_price > last_mms:
        alert_type = "COMPRA" # Cruzamento para cima (Preço rompe MMS)
    elif prev_price > prev_mms and current_price < last_mms:
        alert_type = "VENDA" # Cruzamento para baixo (Preço rompe MMS)
    
    if alert_type:
        # Retorna o dicionário de dados que será usado na inserção SQL
        return alert_type, {
            "ticker": ticker,
            "price": float(current_price),
            "mms": float(last_mms),
            "timestamp": datetime.now().isoformat(),
            "alert_type": alert_type
        }

    return None, None


def insert_alert_into_sql(alert_data: dict, connection_string: str) -> bool:
    """
    Insere o alerta gerado no Banco de Dados SQL do Azure. 
    Ajustado para o schema de 6 colunas.
    """
    
    if not connection_string:
        logging.error("ERRO SQL: String de conexão AZURE_SQL_CONNECTION_STRING não encontrada.")
        return False
        
    TABLE_NAME = 'Alertas' 
    
    insert_query = f"""
    INSERT INTO {TABLE_NAME} (datetime, Ticker, Preco_Fechamento, MMS_Valor, Alerta_Acao, Estrategia)
    VALUES (?, ?, ?, ?, ?, ?);
    """
    
    insert_values = (
        alert_data['timestamp'], 
        alert_data['ticker'],    
        alert_data['price'],     
        alert_data['mms'],       
        alert_data['alert_type'],
        'MMS-14'                 
    )
    
    conn = None
    try:
        # Adiciona o driver explícito, pois a string de conexão do Azure pode ser incompleta
        # Tentamos usar o driver instalado via PRE_BUILD_COMMAND
        full_conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};{connection_string}"
        conn = pyodbc.connect(full_conn_string)
        cursor = conn.cursor()
        
        cursor.execute(insert_query, insert_values) # O pyodbc espera uma tupla de valores
        
        conn.commit()
        logging.info(f"SUCESSO: Alerta {alert_data['alert_type']} para {alert_data['ticker']} inserido no SQL.")
        return True
        
    except pyodbc.Error as e:
        sql_error = f"SQLSTATE: {e.args[0]} | MSG: {e.args[1]}"
        logging.error(f"ERRO SQL CRÍTICO: Falha ao inserir alerta no banco de dados. Detalhe: {sql_error}")
        logging.error("DICA SQL: Verifique se a tabela 'Alertas' e as colunas (datetime, Ticker, etc.) existem e correspondem no Azure SQL.")
        # Se for erro de Driver não encontrado, adicionamos um log claro
        if 'Driver not found' in str(e):
             logging.error("ERRO CRÍTICO: O Driver ODBC não foi encontrado. A instalação do pyodbc falhou no Azure.")
        return False
        
    except Exception as e:
        logging.error(f"ERRO INESPERADO ao inserir alerta: {e}")
        return False
        
    finally:
        if conn:
            conn.close()

def fetch_data_from_brapi(ticker: str, token: str) -> pd.DataFrame:
    """Busca dados de velas do ticker na API Brapi (V2) com retentativas."""
    
    if not token:
        logging.error("ERRO BRAAPI: Token de acesso não fornecido ou vazio.")
        return pd.DataFrame()

    interval = "30m"
    limit = 20 # 20 velas para o cálculo da MMS de 14 períodos
    # Endpoint V2 para velas (necessário para o intervalo de 30m)
    url = f"https://brapi.dev/api/v2/finance/candles/{ticker}" 
    
    params = {
        'interval': interval,
        'limit': limit,
        'token': token
    }
    
    response = None
    
    for attempt in range(MAX_RETRIES):
        logging.info(f"Buscando dados (V2 Candles) para {ticker} (Tentativa {attempt + 1}/{MAX_RETRIES})...")
        
        try:
            response = requests.get(url, params=params, timeout=15)
            
            status_code = response.status_code if response is not None else 'N/A'
            logging.info(f"Resposta HTTP para {ticker}: Status {status_code}")
            
            # Checagem de Erros de Status Específicos para diagnóstico de 404/Token
            if status_code in (404, 401, 403, 429):
                # Usamos um log de erro CLARO para identificar o problema
                if status_code == 404:
                    logging.error(f"ERRO CRÍTICO 404 (Não Encontrado) para {ticker}. Causa: Ticker inválido ou URL '{url}' da Brapi mudou.")
                elif status_code in (401, 403):
                    logging.error(f"ERRO CRÍTICO {status_code} (Token Inválido). Causa: O 'BRAPI_TOKEN' no Azure está incorreto ou expirou.")
                elif status_code == 429:
                    logging.error(f"ERRO CRÍTICO 429 (Limite de Taxa Excedido). Causa: O token gratuito está no Rate Limit. Tente novamente em 1 hora.")
                
                # Se for um erro crítico, pulamos o raise_for_status() e forçamos o retry (se for o caso)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue # Vai para a próxima tentativa
                else:
                    response.raise_for_status() # Força a exceção na última tentativa
            
            # Se não for um erro crítico conhecido, forçamos o raise para capturar outros 4xx/5xx
            response.raise_for_status() 
            
            # Tenta decodificar o JSON
            data = response.json()
            
            if data.get('candles'):
                df = pd.DataFrame(data['candles'])
                df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
                df.set_index('datetime', inplace=True)
                logging.info(f"SUCESSO BRAAPI: {len(df)} velas carregadas para {ticker}.")
                return df
            else:
                logging.warning(f"AVISO BRAAPI: Dados de candles ausentes para {ticker}. Resposta da API: {data}")
                return pd.DataFrame()
                
        except json.JSONDecodeError as e:
            error_response_text = response.text[:300].replace('\n', ' ') if response is not None else "Sem resposta"
            logging.error(f"ERRO JSON DECODIFICAÇÃO para {ticker}: Ocorreu um erro ao ler a resposta da API. {e}. Conteúdo (Início): {error_response_text}...")
                
        except requests.exceptions.HTTPError as errh:
            status_code = response.status_code if response is not None else 'N/A'
            logging.error(f"ERRO HTTP (Tentativa {attempt+1}/{MAX_RETRIES}) para {ticker}: Código {status_code} - {errh}.")
            
        except requests.exceptions.RequestException as err:
            logging.error(f"ERRO DE REQUISIÇÃO GENÉRICO (Tentativa {attempt+1}/{MAX_RETRIES}) para {ticker}: {err}")
        except Exception as e:
            logging.error(f"ERRO INESPERADO (Tentativa {attempt+1}/{MAX_RETRIES}) ao buscar dados para {ticker}: {e}")
        
        # Se falhou, mas não foi a última tentativa, espera e tenta novamente
        if attempt < MAX_RETRIES - 1:
            wait_time = 2 ** attempt 
            logging.info(f"Falha na tentativa {attempt + 1}. Tentando novamente em {wait_time} segundos...")
            time.sleep(wait_time)


    # Se todas as tentativas falharem
    logging.error(f"FALHA PERMANENTE: Não foi possível obter dados da Brapi para {ticker} após {MAX_RETRIES} tentativas.")
    return pd.DataFrame()


@app.function_name(name="timer_trigger")
# CRON alterado para rodar a cada 5 minutos para facilitar o debugging
@app.schedule(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    """Função disparada por tempo para executar a análise de MMS e SQL."""
    
    # 0. Checagem de ambiente
    if not CONNECTION_STRING:
        logging.critical("ERRO FATAL: AZURE_SQL_CONNECTION_STRING não está configurada.")
        return

    if not BRAPI_TOKEN:
        logging.critical("ERRO FATAL: BRAPI_TOKEN não está configurada.")
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
                if insert_alert_into_sql(alert_data, CONNECTION_STRING):
                    alerts_generated += 1
            else:
                logging.info(f"Nenhum cruzamento de MMS (Alerta) detectado para {ticker}.")
                
        else:
            logging.info(f"Ignorando {ticker} devido à falha ou ausência de dados.")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logging.info(f'--- EXECUÇÃO CONCLUÍDA ---')
    logging.info(f'Total de alertas gerados: {alerts_generated}')
    logging.info(f'Duração total: {duration:.2f} segundos')
    
    if alerts_generated > 0:
        logging.info(f"SUCESSO: {alerts_generated} alerta(s) inserido(s) no Banco de Dados SQL.")
    else:
        logging.info("Nenhum alerta gerado ou inserido nesta execução.")