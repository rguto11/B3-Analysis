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
    verificar o cruzamento e registrar a análise no log do Azure e no SQL DB.
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
    # 2. Coleta Múltipla de Dados (Uma requisição para todos os tickers)
    # --------------------------------------------------------------------------

    # Converte TICKERS para uma string separada por vírgulas para a API
    tickers_str = ",".join(TICKERS)
    
    # URL da API da Brapi
    url_brapi = f"https://brapi.dev/api/quote/{tickers_str}"
    
    # Parâmetros para buscar o histórico
    params = {
        'interval': INTERVALO,
        'days': DIAS,
        'token': TOKEN_BRAPI,
        'output': 'json',
        'fields': 'historicalData'
    }

    try:
        response = requests.get(url_brapi, params=params, timeout=30)
        response.raise_for_status() # Lança exceção para códigos de status HTTP de erro
        dados_json = response.json()
        
        for resultado in dados_json.get('results', []):
            ticker = resultado.get('symbol')
            historical_data = resultado.get('historicalData', [])
            
            if historical_data:
                # Cria o DataFrame
                df = pd.DataFrame(historical_data)
                
                # Converte o timestamp para datetime (UTC e depois para SP)
                df['datetime'] = pd.to_datetime(df['date'], unit='s', utc=True).dt.tz_convert('America/Sao_Paulo')
                
                # Remove colunas desnecessárias, renomeia e ajusta tipos
                df = df[['datetime', 'open', 'close', 'high', 'low', 'volume']]
                df.columns = ['datetime', 'Open', 'Close', 'High', 'Low', 'Volume']
                df['Ticker'] = ticker # Adiciona a coluna Ticker
                
                # Garante que 'Close' é float para o cálculo da MMS
                df['Close'] = df['Close'].astype(float)
                
                # Adiciona o DF processado à lista
                todos_os_dataframes.append(df)
                
                logging.info(f"SUCESSO: Dados de {ticker} coletados ({len(df)} registros).")
            else:
                logging.warning(f"AVISO: Nenhuma 'historicalData' encontrada para {ticker}.")

    except requests.exceptions.Timeout:
        logging.error("ERRO: O tempo limite da requisição HTTP para a Brapi foi atingido.")
        return
    except requests.exceptions.RequestException as e:
        logging.error(f"ERRO HTTP: Falha na requisição para a Brapi: {e}")
        return
    except Exception as e:
        logging.error(f"ERRO GERAL na coleta de dados: {e}")
        return

    # --------------------------------------------------------------------------
    # 3. Análise, Geração de Alertas e Extração Inicial de Preços
    # --------------------------------------------------------------------------

    registros_para_sql = []
    primeiros_precos = [] # Lista para armazenar o preço e MMS mais recentes de cada ativo
    
    for df in todos_os_dataframes:
        if df.empty:
            continue
        
        ticker = df.iloc[0]['Ticker']
        
        # 3.1. Cálculo da Média Móvel Simples (MMS)
        df['MMS_Valor'] = df['Close'].rolling(window=PERIODO_MMS).mean()
        
        # Remove as linhas iniciais com valor NaN no MMS
        df.dropna(subset=['MMS_Valor'], inplace=True)

        # ----------------------------------------------------------------------
        # EXTRAÇÃO DOS 2 PREÇOS MAIS RECENTES (Último Preço e Última MMS)
        # ----------------------------------------------------------------------
        if not df.empty:
            ultima_linha = df.iloc[-1]
            preco_fechamento_atual = ultima_linha['Close']
            mms_valor_atual = ultima_linha['MMS_Valor']
            
            primeiros_precos.append((
                ticker, 
                f"R${preco_fechamento_atual:.2f}", 
                f"R${mms_valor_atual:.2f}"
            ))
        else:
            # Pula para o próximo ticker se o DF ficou vazio após o dropna
            logging.warning(f"AVISO: {ticker} possui dados insuficientes após o cálculo da MMS.")
            continue


        # 3.2. Verificação de Cruzamento (Alerta)
        
        # Necessita de pelo menos 2 pontos para verificar o cruzamento
        if len(df) < 2:
            logging.warning(f"AVISO: {ticker} possui dados insuficientes após o cálculo da MMS para verificar o cruzamento.")
            continue

        # Cria colunas para o ponto anterior
        df['Close_Anterior'] = df['Close'].shift(1)
        df['MMS_Anterior'] = df['MMS_Valor'].shift(1)
        
        # Ação padrão: Aguardar (inclui NaN na primeira linha)
        df['Alerta_Acao'] = 'Aguardar'
        
        # Verifica o sinal de cruzamento de compra (Preço anterior abaixo da MMS, Preço atual acima ou igual)
        condicao_compra = (df['Close_Anterior'] < df['MMS_Anterior']) & (df['Close'] >= df['MMS_Valor'])
        df.loc[condicao_compra, 'Alerta_Acao'] = 'COMPRAR'

        # Verifica o sinal de cruzamento de venda (Preço anterior acima da MMS, Preço atual abaixo)
        condicao_venda = (df['Close_Anterior'] >= df['MMS_Anterior']) & (df['Close'] < df['MMS_Valor'])
        df.loc[condicao_venda, 'Alerta_Acao'] = 'VENDER'

        # 3.3. Preparação dos Registros
        # Filtra apenas os registros que geraram alerta (COMPRAR ou VENDER)
        df_alertas = df[df['Alerta_Acao'].isin(['COMPRAR', 'VENDER'])].copy()
        
        if not df_alertas.empty:
            logging.info(f"ALERTA(S) GERADO(S) para {ticker}: {df_alertas['Alerta_Acao'].tolist()}")
            
            # Formata os registros para a inserção no SQL
            registros = df_alertas.apply(
                lambda row: (
                    row['datetime'].strftime('%Y-%m-%d %H:%M:%S'), # Formata o datetime para SQL
                    ticker,
                    row['Close'],
                    row['MMS_Valor'],
                    row['Alerta_Acao'],
                    'Cruzamento MMS'
                ),
                axis=1
            ).tolist()
            registros_para_sql.extend(registros)

    # --------------------------------------------------------------------------
    # 4. Sumário dos Preços Iniciais (Os 2 Preços - Logado no Azure)
    # --------------------------------------------------------------------------
    if primeiros_precos:
        log_msg = ["ÚLTIMOS PREÇOS E MMS (2 Preços por Ticker) antes de abastecer a base:"]
        for ticker, preco, mms in primeiros_precos:
            log_msg.append(f"  - {ticker}: Preço = {preco}, MMS({PERIODO_MMS}) = {mms}")
        
        logging.info("\n".join(log_msg))
    else:
        logging.warning("AVISO: Nenhum preço inicial de fechamento/MMS foi extraído.")
        
    # --------------------------------------------------------------------------
    # 5. Persistência dos Dados (Abastecendo a Base SQL)
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
        logging.warning("AVISO: String de Conexão SQL não fornecida. Alertas não foram persistidos no Banco de Dados.")
    
    else:
        logging.info("INFO: Nenhum alerta de COMPRA/VENDA gerado para inserção no SQL.")

    logging.info("Execução da Análise de Cruzamento de MMS concluída.")