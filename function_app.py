import os
import requests
import json
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import logging
from pytz import timezone

# Configuração do Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES DE VARIÁVEIS DE AMBIENTE ---
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BRAPI_TOKEN = os.getenv("BRAPI_TOKEN")
TICKERS = ['PETR4', 'MGLU3', 'VALE3', 'ITUB4']
INTERVALO = '30m'
DIAS = 5
BLOB_CONTAINER_NAME = "alerts"
ROLLING_AVERAGE_PERIOD = 20 # Período para o cálculo da MMS (Média Móvel Simples)

# --- FUNÇÕES CORE ---

def fetch_brapi_data(ticker: str) -> dict:
    """Busca dados históricos de cotação para um único ticker na API Brapi."""
    logger.info(f"Iniciando coleta para: {ticker}")
    url = f"https://brapi.dev/api/quote/{ticker}"
    
    # Parâmetros da API: CRÍTICO usar historical=True para pegar o OHLCV
    params = {
        'token': BRAPI_TOKEN,
        'interval': INTERVALO,
        'range': f'{DIAS}d',
        'historical': True
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status() # Lança exceção para códigos de erro HTTP
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro na requisição Brapi para {ticker}: {e}")
        return {}

def save_to_blob_storage(data: str, blob_name: str):
    """Salva a string de dados como um blob no Azure Blob Storage."""
    if not AZURE_STORAGE_CONNECTION_STRING:
        logger.error("AZURE_STORAGE_CONNECTION_STRING não configurada.")
        return
        
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(
            container=BLOB_CONTAINER_NAME, 
            blob=blob_name
        )
        
        blob_client.upload_blob(data, overwrite=True)
        logger.info(f"✅ Arquivo salvo no Blob Storage: {blob_name}")
    except Exception as e:
        logger.error(f"Erro ao salvar no Blob Storage: {e}")

def run_analysis():
    """Função principal: coleta, processa, calcula MMS e salva no Blob Storage."""
    
    if not all([BRAPI_TOKEN, AZURE_STORAGE_CONNECTION_STRING]):
        logger.error("Variáveis de ambiente BRAPI_TOKEN ou AZURE_STORAGE_CONNECTION_STRING não estão definidas.")
        return

    all_data = []
    
    # 2. Coleta de Dados Históricos
    for ticker in TICKERS:
        data = fetch_brapi_data(ticker)
        
        # O foco é no 'historicalData', ignorando os dados de cotação atual (que não têm OHLCV por 30m)
        if data and data.get('historicalData'):
            df = pd.DataFrame(data['historicalData'])
            
            # Adiciona o ticker
            df['ticker'] = ticker
            
            # --- CRÍTICO: CONVERSÃO DE TIMESTAMP PARA DATETIME ---
            df['datetime'] = pd.to_datetime(df['date'], unit='s').dt.tz_localize(None) 
            
            # Renomeia colunas para o esquema que queremos
            df = df.rename(columns={'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'})
            
            # Filtra apenas as colunas que serão usadas no cálculo
            df = df[['ticker', 'datetime', 'open', 'high', 'low', 'close', 'volume']]
            
            all_data.append(df)
        else:
            logger.warning(f"Dados históricos vazios ou ausentes para {ticker}")

    if not all_data:
        logger.error("Nenhum dado histórico coletado. Finalizando a execução.")
        return

    # 3. Consolidação e Preparação do DataFrame
    df_master = pd.concat(all_data, ignore_index=True).dropna(subset=['close'])

    # --- LÓGICA DE ALERTA (MMS E STATUS) ---
    
    def calculate_alerts(group):
        """Calcula a MMS e gera o status de alerta (BUY/SELL) para um grupo (ticker)."""
        group = group.sort_values(by='datetime')
        
        # 1. Cálculo da Média Móvel Simples (MMS)
        group['mms'] = group['close'].rolling(window=ROLLING_AVERAGE_PERIOD, min_periods=1).mean()
        
        # 2. Geração do Status de Alerta
        group['status'] = 'HOLD'
        
        # Sinal de Compra (BUY)
        buy_condition = (group['close'] > group['mms']) & (group['close'].shift(1) <= group['mms'].shift(1))
        group.loc[buy_condition, 'status'] = 'BUY'
        
        # Sinal de Venda (SELL)
        sell_condition = (group['close'] < group['mms']) & (group['close'].shift(1) >= group['mms'].shift(1))
        group.loc[sell_condition, 'status'] = 'SELL'
        
        # Se for a última cotação e não houver BUY/SELL, mantém como NEUTRO (melhor que HOLD)
        if group['status'].iloc[-1] not in ['BUY', 'SELL']:
            group.loc[group.index[-1], 'status'] = 'NEUTRO'
            
        return group
    
    # Aplica a função de alerta para cada ticker separadamente
    df_master = df_master.groupby('ticker').apply(calculate_alerts, include_groups=False).reset_index(drop=True)
    
    # 4. Formato de Saída (Filtro Final CRÍTICO)
    
    # Define as colunas finais na ORDEM correta
    colunas_finais = ['ticker', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'mms', 'status']
    df_final = df_master[colunas_finais]
    
    # Nome do arquivo CSV
    sao_paulo_tz = timezone('America/Sao_Paulo')
    current_time_sp = datetime.now(sao_paulo_tz)
    timestamp_str = current_time_sp.strftime("%Y-%m-%d_%H-%M-%S")
    csv_blob_name = f"analise_b3_{timestamp_str}.csv"
    
    # Salva o DataFrame final no Blob Storage
    csv_data = df_final.to_csv(index=False)
    save_to_blob_storage(csv_data, csv_blob_name)
    
    logger.info(f"✅ Análise concluída. Total de registros processados: {len(df_final)}")


# --- PONTO DE ENTRADA DO CONTAINER APP JOB ---
if __name__ == "__main__":
    logger.info("Iniciando a execução do Container App Job.")
    run_analysis()
    logger.info("Execução do Container App Job finalizada.")