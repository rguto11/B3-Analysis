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
DAYS_TO_FETCH = 5 # Usa 5 dias de histórico (suficiente para velas de 30m)

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
    if len(df.dropna(subset=['close'])) < period:
        logging.warning(f"AVISO: Dados insuficientes para {ticker}. Requer {period} pontos, mas encontrou {len(df.dropna(subset=['close']))}.")
        return None, None

    # 2. Cálculo da MMS
    df['MMS'] = df['close'].rolling(window=period).mean()

    # Garantir que haja pelo menos dois pontos válidos (atual e anterior)
    if len(df.dropna(subset=['close', 'MMS'])) < 2:
        logging.info(f"Dados insuficientes para análise de cruzamento de MMS para {ticker}.")
        return None, None

    # 3. Extração dos pontos atuais e anteriores
    last_row = df.iloc[-1]
    prev_row = df.iloc[-2]
    
    current_price = last_row['close']
    last_mms = last_row['MMS']
    
    prev_price = prev_row['close']
    prev_mms = prev_row['MMS']
    
    alert_type = None
    
    # Validação de dados (caso o último ponto seja NaN)
    if pd.isna(current_price) or pd.isna(last_mms) or pd.isna(prev_price) or pd.isna(prev_mms):
        logging.warning(f"AVISO: Dados de preço ou MMS inválidos para análise de cruzamento em {ticker}.")
        return None, None
        
    # 4. Verificação de Alerta (Cruzamento)
    if prev_price < prev_mms and current_price > last_mms:
        alert_type = "COMPRA"
    elif prev_price > prev_mms and current_price < last_mms:
        alert_type = "VENDA"
    
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
    Ajustado para o schema de 6 colunas (datetime, Ticker, Preco_Fechamento, MMS_Valor, Alerta_Acao, Estrategia).
    """
    
    if not connection_string:
        logging.error("ERRO SQL: String de conexão AZURE_SQL_CONNECTION_STRING não encontrada.")
        return False
        
    # Nome da Tabela: Ajuste para 'Alertas' (baseado nas conversas anteriores)
    TABLE_NAME = 'Alertas' 
    
    # Query de inserção com 6 colunas
    insert_query = f"""
    INSERT INTO {TABLE_NAME} (datetime, Ticker, Preco_Fechamento, MMS_Valor, Alerta_Acao, Estrategia)
    VALUES (?, ?, ?, ?, ?, ?);
    """
    
    # Dados para inserção (6 campos, Estrategia é fixo)
    insert_values = (
        alert_data['timestamp'], # 1. datetime (string ISO, o SQL Server converterá)
        alert_data['ticker'],    # 2. Ticker
        alert_data['price'],     # 3. Preco_Fechamento
        alert_data['mms'],       # 4. MMS_Valor
        alert_data['alert_type'],# 5. Alerta_Acao
        'MMS-14'                 # 6. Estrategia (Valor Fixo)
    )
    
    conn = None
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Executa a inserção com parâmetros seguros 
        cursor.execute(insert_query, *insert_values)
        
        conn.commit()
        logging.info(f"SUCESSO: Alerta {alert_data['alert_type']} para {alert_data['ticker']} inserido no SQL.")
        return True
        
    except pyodbc.Error as e:
        # Erro específico de SQL/Driver COM LOG COMPLETO para diagnóstico
        sql_error = f"SQLSTATE: {e.args[0]} | MSG: {e.args[1]}"
        logging.error(f"ERRO SQL CRÍTICO: Falha ao inserir alerta no banco de dados. Detalhe: {sql_error}")
        logging.error(f"Query usada: {insert_query.strip()} | Valores: {insert_values}")
        logging.error("DICA: Verifique se 'TrustServerCertificate=yes' está na string de conexão e se a tabela/colunas existem no seu Azure SQL.")
        return False
        
    except Exception as e:
        logging.error(f"ERRO INESPERADO ao inserir alerta: {e}")
        return False
        
    finally:
        if conn:
            conn.close()

def fetch_data_from_brapi(ticker: str, token: str) -> pd.DataFrame:
    """
    Busca dados de velas do ticker na API Brapi (usando o endpoint V1 estável) 
    com retentativas (backoff).
    """
    
    # Verifica o token no início
    if not token:
        logging.error("ERRO BRAAPI: Token de acesso não fornecido ou vazio.")
        return pd.DataFrame()

    # MUDANÇA CRÍTICA: Revertendo para o endpoint V1 (/api/quote)
    url = f"https://brapi.dev/api/quote/{ticker}" 
    
    params = {
        'interval': "30m",
        'days': DAYS_TO_FETCH, # Usado no V1 para profundidade histórica
        'token': token,
        'fields': 'historicalData' # CRÍTICO para obter a série histórica no V1
    }
    
    response = None
    
    for attempt in range(MAX_RETRIES):
        logging.info(f"Buscando dados em tempo real (V1) para {ticker} (Tentativa {attempt + 1}/{MAX_RETRIES})...")
        
        try:
            response = requests.get(url, params=params, timeout=15)
            
            # Log do Status HTTP da resposta
            status_code = response.status_code if response is not None else 'N/A'
            logging.info(f"Resposta HTTP para {ticker}: Status {status_code}")
            
            response.raise_for_status() 
            
            # Tenta decodificar o JSON
            data = response.json()
            
            # V1 Response: Acha o resultado do ticker e extrai 'historicalData'
            if data.get('results'):
                result = data['results'][0] # O resultado está na primeira (e única) posição
                historical_data = result.get('historicalData')
                
                if historical_data:
                    df = pd.DataFrame(historical_data)
                    df['datetime'] = pd.to_datetime(df['date'], unit='s') # V1 usa 'date' (timestamp)
                    df.set_index('datetime', inplace=True)
                    # O nome da coluna no V1 já é 'close' (minúsculo), que é o que o calculate_mms_and_alerts espera.
                    logging.info(f"SUCESSO BRAAPI (V1): {len(df)} velas carregadas para {ticker}.")
                    return df
                else:
                    logging.warning(f"AVISO BRAAPI: Dados 'historicalData' ausentes para {ticker}. Resposta da API: {data}")
                    return pd.DataFrame()
            else:
                 logging.warning(f"AVISO BRAAPI: Array 'results' ausente ou vazio para {ticker}. Resposta: {data}")
                 return pd.DataFrame()
                
        except json.JSONDecodeError as e:
            # Tratamento para erro de JSON (ocorre se for 404/403 com resposta HTML/Texto)
            error_response_text = response.text[:300].replace('\n', ' ') if response is not None else "Sem resposta"
            
            if response is not None and (not response.text or response.text.strip().startswith('<!DOCTYPE html>')):
                logging.error(f"ERRO DE CONTEÚDO (TOKEN/LIMIT) para {ticker}: A API Brapi retornou HTML ou VAZIO. **BRAPI_TOKEN inválido ou Rate Limit Excedido**. Conteúdo (Início): {error_response_text}...")
            else:
                logging.error(f"ERRO JSON DECODIFICAÇÃO para {ticker}: {e}. Conteúdo (Início): {error_response_text}...")
                
        except requests.exceptions.HTTPError as errh:
            # Captura erros 4xx/5xx (incluindo 404, 401, 429)
            status_code = response.status_code if response is not None else 'N/A'
            logging.error(f"ERRO HTTP (Tentativa {attempt+1}/{MAX_RETRIES}) para {ticker}: Código {status_code} - {errh}")
            
        except requests.exceptions.RequestException as err:
            # Captura erros de conexão, timeout, DNS, etc.
            logging.error(f"ERRO DE REQUISIÇÃO GENÉRICO (Tentativa {attempt+1}/{MAX_RETRIES}) para {ticker}: {err}")
        except Exception as e:
            logging.error(f"ERRO INESPERADO (Tentativa {attempt+1}/{MAX_RETRIES}) ao buscar dados para {ticker}: {e}")
        
        # Se falhou, mas não foi a última tentativa, espera e tenta novamente (Exponential Backoff)
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
        logging.critical("Verifique se a variável está na aba 'Cadeias de Conexão' no Azure Portal e se o tipo é SQLAzure.")
        return

    # A checagem do token BRAPI é feita aqui e dentro de fetch_data_from_brapi
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
                # Passa a string de conexão para a função de inserção
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