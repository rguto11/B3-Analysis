# B3-Analysis — AlertaMMS

Sistema de monitoramento em nuvem para análise de ações brasileiras com alertas inteligentes baseados em Média Móvel Simples (MMS).

---

## Descrição Geral

Este projeto consiste em uma **solução completa de nuvem para Previsão de Alertas de Ações**. O objetivo é monitorar cotações da B3 em tempo quase-real e gerar sinais de compra/venda automaticamente.

O sistema recebe dados brutos (CSV) da API Brapi, processa automaticamente utilizando computação serverless (Azure Container Apps Job) para aplicar o indicador MMS, e armazena os resultados processados para visualização no Power BI.

### O que o sistema faz:

-  Coleta cotações de 4+ ativos brasileiros a cada 30 minutos
-  Calcula Média Móvel Simples (MMS) de 20 períodos
-  Gera alertas automáticos (BUY/SELL/HOLD)
-  Armazena histórico em CSV no Azure Blob Storage
-  Disponibiliza dados para visualização no Power BI

---

##  Arquitetura da Solução

```
┌─────────────────────────────────────────────────────────┐
│              API Brapi / yfinance                       │
│          (Cotações B3 - 30min)                          │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│         Azure Container Registry                        │
│         (Imagem Docker - Python)                        │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼ (Cron Job: a cada hora)
┌─────────────────────────────────────────────────────────┐
│    Azure Container Apps Job                            │
│  • Coleta dados Brapi                                  │
│  • Calcula MMS (20 períodos)                           │
│  • Gera alertas (BUY/SELL/HOLD)                        │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│     Azure Blob Storage                                 │
│     • Container: alerts                                │
│     • Arquivos CSV com timestamp                       │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│              Power BI                                  │
│         (Dashboards e Visualizações)                   │
└─────────────────────────────────────────────────────────┘
```

### Componentes Principais

| Componente | Função | Tecnologia |
|-----------|--------|-----------|
| **API Brapi** | Fornece cotações | REST API |
| **Container Registry** | Armazena imagem Docker | Azure ACR |
| **Container Apps Job** | Executa processamento | Python 3.9 + Pandas |
| **Blob Storage** | Salva alertas (CSV) | Azure Storage |
| **Power BI** | Visualiza resultados | BI Dashboard |

---

## Dataset

**Fonte de Dados:**
- API: Brapi (API pública para B3)
- Período: 5 dias
- Frequência: 30 minutos
- Volume de Dados: ~197 registros/execução

**Ativos Monitorados:**
- PETR4 (Petrobras)
- MGLU3 (Magazine Luiza)
- VALE3 (Vale)
- ITUB4 (Itaú Unibanco)

**Colunas Principais:**
- `time` — Data/hora da cotação
- `open` — Preço de abertura
- `high` — Preço máximo
- `low` — Preço mínimo
- `close` — Preço de fechamento
- `volume` — Volume negociado

---

## Como Usar

### 1. Pré-requisitos

- Conta no Azure (free tier)
- Docker instalado
- Git
- Python 3.9+

### 2. Clonar o Repositório

```bash
git clone https://github.com/rguto11/B3-Analysis.git
cd B3-Analysis
```

### 3. Configurar Variáveis de Ambiente

Crie um arquivo `.env`:

```env
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...
BRAPI_TOKEN=o5yKDG4WEqfnLWuyAAbeUy
```

### 4. Build da Imagem Docker

```bash
docker build -t alertamms:latest .
```

### 5. Deploy no Azure

```bash
# Login no Azure
az login

# Criar Container Registry
az acr create --resource-group seu-rg --name seu-acr --sku Basic

# Push da imagem
az acr build --registry seu-acr --image alertamms:latest .

# Criar Container Apps Job
az containerapp job create \
  --name alertamms-job \
  --resource-group seu-rg \
  --environment seu-env \
  --trigger-type Schedule \
  --replica-timeout 600 \
  --replica-retry-limit 1 \
  --cron-expression "0 * * * *" \
  --image seu-acr.azurecr.io/alertamms:latest
```

---

## Estrutura do Projeto

```
B3-Analysis/
├── AlertaMMS/
│   ├── function_app.py          # Lógica principal
│   ├── requirements.txt         # Dependências
│   ├── function.json            # Config Azure
│   └── host.json               # Runtime config
├── .github/
│   └── workflows/
│       └── main_b3-mms-func-app.yml
├── Dockerfile                   # Container
├── .env.example                # Template variáveis
├── README.md                   # Este arquivo
└── requirements.txt
```

---

## Alertas e Sinais

O sistema gera 3 tipos de sinais baseados no cruzamento de preço com MMS:

### BUY (Compra)
Quando o preço cruza a MMS de **baixo para cima**

```
Período anterior: Preço ≤ MMS
Período atual:    Preço > MMS
```

### SELL (Venda)
Quando o preço cruza a MMS de **cima para baixo**

```
Período anterior: Preço ≥ MMS
Período atual:    Preço < MMS
```

### HOLD (Manter)
Quando **não há cruzamento** recente

---

## Armazenamento de Dados

Os alertas são salvos em CSV no Azure Blob Storage com timestamp:

```
alerts_2025-11-27_14-30-00.csv
alerts_2025-11-27_15-30-00.csv
alerts_2025-11-27_16-30-00.csv
```

**Exemplo de arquivo gerado:**

```csv
ticker,datetime,status,close,mms,periodo_mms
PETR4,2025-11-27T14:30:00,HOLD,32.28,32.26,20
MGLU3,2025-11-27T14:30:00,BUY,10.19,9.97,20
VALE3,2025-11-27T14:30:00,HOLD,65.65,65.48,20
ITUB4,2025-11-27T14:30:00,HOLD,39.91,39.87,20
```

---

## Visualização - Power BI

O Power BI conecta diretamente ao Blob Storage e exibe:

- **Gráfico de Tendência:** Preço vs MMS ao longo do tempo
- **Métricas:** MAE (Erro Médio Absoluto), RMSE
- **Alertas Recentes:** Últimos sinais gerados
- **Série Histórica:** Todas as execuções

---

## Troubleshooting

### Erro: "Imagem Docker não encontrada"
```bash
# Verifique se a imagem está no Azure ACR
az acr repository list --name seu-acr
```

### Erro: "Container Job não está rodando"
```bash
# Verifique os logs
az containerapp job execution logs -n alertamms-job -g seu-rg
```

### Erro: "Storage connection string inválida"
```bash
# Obtenha a connection string correta
az storage account show-connection-string -g seu-rg -n seu-storage
```

### Erro: "API Brapi retornou erro 429"
Aguarde alguns minutos (rate limit) e execute novamente.

---

## Arquivo Principal: `AlertaMMS/function_app.py`

```python
import azure.functions as func
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
from datetime import datetime

app = func.FunctionApp()

@app.timer_trigger(arg_name="myTimer", schedule="0 * * * *")
def alertamms_job(myTimer: func.TimerRequest):
    """Executa coleta, processamento e persistência a cada hora"""
    
    # 1. Coleta dados Brapi
    df = coletar_brapi()
    
    # 2. Calcula MMS
    df['MMS'] = df['close'].rolling(window=20).mean()
    
    # 3. Gera alertas
    alertas = gerar_alertas(df)
    
    # 4. Salva em CSV
    salvar_csv(alertas)
    
    func.get_logger().info(f"✅ {len(alertas)} alertas gerados")
```

---

## Equipe

- **Artur De Paola Prieto Carvalho** 
- **Augusto Melo Ribeiro** 

---

## Referências

- [Documentação Azure Functions](https://learn.microsoft.com/en-us/azure/azure-functions/)
- [Brapi API](https://brapi.dev/docs)
- [Pandas Rolling Mean](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.rolling.html)
- [Azure Container Apps Jobs](https://learn.microsoft.com/en-us/azure/container-apps/jobs)
- [Power BI Azure Storage](https://learn.microsoft.com/en-us/power-bi/connect-data/service-azure-sql-database-with-direct-query)

---

## Licença

MIT License — Veja `LICENSE` para detalhes.

---
