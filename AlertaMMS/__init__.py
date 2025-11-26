import logging
import os
import requests
import pyodbc
import pandas as pd
import datetime
import azure.functions as func

def main(mytimer: func.TimerRequest) -> None:
    """
    Função Time Trigger para coletar dados da Brapi, calcular MMS (20),
    verificar o cruzamento e registrar a análise no log do Azure.
    """
    # --------------------------------------------------------------------------
    # 1. Configurações e Variáveis de Ambiente
    # --------------------------------------------------------------------------
    
    # Obtém as variáveis de ambiente que você configurou no Azure Portal
    TOKEN_BRAPI = os.environ.get('BRAPI_TOKEN')
    SQL_CONNECTION_STRING = os.environ.get('AZURE_SQL_CONNECTION_STRING')
    
    # Configurações do projeto
    TICKERS = ['PETR4', 'MGLU3', 'VALE3', 'ITUB4']
    INTERVALO = '30m'
    DIAS = 5
    PERIODO_MMS = 20
    
    if not TOKEN_BRAPI:
        logging.error("O token da Brapi não foi encontrado nas Variáveis de Ambiente. A execução será interrompida.")
        return
        
    logging.info(f"Iniciando coleta de {len(TICKERS)} ativos na frequência de {INTERVALO}...")

    todos_os_dataframes = []

    # --------------------------------------------------------------------------
    # 2. Coleta Múltipla com Brapi
    # --------------------------------------------------------------------------
    
    for TICKER in TICKERS:
        URL_BASE = f'https://brapi.dev/api/quote/{TICKER}?range={DIAS}d&interval={INTERVALO}&token={TOKEN_BRAPI}'
        df_temp = pd.DataFrame()

        try:
            resposta = requests.get(URL_BASE)
            resposta.raise_for_status()
            dados_json = resposta.json()

            if 'results' in dados_json and dados_json['results']:
                dados_historico = dados_json['results'][0]['historicalDataPrice']
                df_temp = pd.DataFrame(dados_historico)

                # Processamento do DataFrame
                df_temp.rename(columns={'date': 'datetime'}, inplace=True)
                df_temp['Ticker'] = TICKER
                df_temp['datetime'] = pd.to_datetime(df_temp['datetime'], unit='s')
                df_temp.set_index('datetime', inplace=True)
                
                colunas_numericas = ['open', 'high', 'low', 'close', 'volume']
                df_temp[colunas_numericas] = df_temp[colunas_numericas].astype(float)
                
                todos_os_dataframes.append(df_temp)
                logging.info(f"Sucesso: {TICKER} coletado ({df_temp.shape[0]} registros).")

            else:
                logging.warning(f"Falha: Não há resultados para o ticker {TICKER}.")

        except requests.exceptions.RequestException as e:
            logging.error(f"Erro de Requisição para {TICKER}: {e}")

    df_master = pd.concat(todos_os_dataframes) if todos_os_dataframes else pd.DataFrame()

    if df_master.empty:
        logging.error("ERRO: df_master Vazio. Coleta não gerou dados. Interrompendo.")
        return

    logging.info(f"Coleta Múltipla Concluída. Total de Registros: {df_master.shape[0]}.")

    # --------------------------------------------------------------------------
    # 3. Processamento, Cálculo da MMS e Alerta
    # --------------------------------------------------------------------------
    
    # Cálculo da MMS (adaptado para MultiIndex)
    mms_series = df_master.groupby('Ticker')['close'].rolling(window=PERIODO_MMS).mean()
    mms_series.name = 'MMS'
    df_mms = mms_series.reset_index(level=['Ticker', 'datetime'])
    
    df_master_reset = df_master.reset_index()
    df_master = df_master_reset.merge(
        df_mms[['Ticker', 'datetime', 'MMS']],
        on=['Ticker', 'datetime'],
        how='left'
    ).set_index('datetime')
    
    logging.info("ANÁLISE DE ALERTA INTELIGENTE (Cruzamento MMS) iniciada.")

    # 4. Iterar e Enviar Alerta/Inserir no SQL
    
    registros_para_sql = []
    
    for ticker in df_master['Ticker'].unique():
        df_ticker = df_master[df_master['Ticker'] == ticker].dropna(subset=['MMS'])

        if df_ticker.shape[0] < 2:
            logging.warning(f"[AVISO] {ticker}: Dados insuficientes ({df_ticker.shape[0]} para MMS).")
            continue

        registro_recente = df_ticker.iloc[-1]
        registro_anterior = df_ticker.iloc[-2]

        preco_atual = registro_recente['close']
        mms_atual = registro_recente['MMS']
        preco_anterior = registro_anterior['close']
        mms_anterior = registro_anterior['MMS']
        
        alerta_acao = "Nenhum"
        
        if preco_atual > mms_atual and preco_anterior <= mms_anterior:
            alerta_acao = "COMPRA"
            logging.info(f"!!! ALERTA {ticker}: Preço ({preco_atual:.2f}) CRUZOU MMS ({mms_atual:.2f}) de baixo para cima (COMPRA).")
        elif preco_atual < mms_atual and preco_anterior >= mms_anterior:
            alerta_acao = "VENDA"
            logging.info(f"!!! ALERTA {ticker}: Preço ({preco_atual:.2f}) CRUZOU MMS ({mms_atual:.2f}) de cima para baixo (VENDA).")
        else:
            logging.info(f"--- {ticker}: Sem cruzamento recente. Tendência mantida. ---")

        # Preparar registro para o SQL
        if alerta_acao != "Nenhum":
            registros_para_sql.append((
                registro_recente.name, # datetime
                ticker,
                preco_atual,
                mms_atual,
                alerta_acao,
                'MMS-20'
            ))

    # --------------------------------------------------------------------------
    # 5. Inserção no Banco de Dados SQL do Azure
    # --------------------------------------------------------------------------
    
    if registros_para_sql and SQL_CONNECTION_STRING:
        try:
            cnxn = pyodbc.connect(SQL_CONNECTION_STRING)
            cursor = cnxn.cursor()
            
            # Tabela de Alertas (Você pode ajustar o nome da tabela)
            TABLE_NAME = 'Alertas'
            
            for registro in registros_para_sql:
                # O comando SQL deve refletir a estrutura da sua tabela Alertas
                sql_insert = f"""
                INSERT INTO {TABLE_NAME} 
                (datetime, Ticker, Preco_Fechamento, MMS_Valor, Alerta_Acao, Estrategia) 
                VALUES (?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql_insert, *registro)
            
            cnxn.commit()
            logging.info(f"SUCESSO: {len(registros_para_sql)} alerta(s) inserido(s) no Banco de Dados SQL.")
            
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            logging.error(f"ERRO PYODBC: Falha na inserção no SQL: {sqlstate}. Verifique a string de conexão ou permissões.")
        
        finally:
            if 'cnxn' in locals() and cnxn:
                cnxn.close()
                logging.info("Conexão SQL fechada.")
    
    elif not SQL_CONNECTION_STRING:
        logging.warning("AVISO: String de Conexão SQL não configurada. Alertas não serão salvos no banco de dados.")

    logging.info("Execução da Função Azure concluída.")