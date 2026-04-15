# RPA PROVER - Automação de Extração de Dados Financeiros

Sistema automatizado de extração, processamento e análise de dados financeiros do sistema PROVER.

## 📋 Visão Geral

Este projeto implementa um RPA (Robotic Process Automation) para automatizar a coleta de dados financeiros do sistema PROVER, processá-los seguindo a arquitetura medalhão (Bronze/Silver/Gold) e carregá-los no BigQuery para análise.

## 🏗️ Arquitetura

```
┌─────────────┐     ┌─────────┐     ┌──────────┐     ┌─────────┐
│   PROVER    │ --> │   RPA   │ --> │   GCS    │ --> │ BigQuery│
│   (Fonte)   │     │ Selenium│     │ (Storage)│     │ (Análise)│
└─────────────┘     └─────────┘     └──────────┘     └─────────┘
                                           │
                                    ┌──────┴──────┐
                                    │   Camadas:  │
                                    │  - Bronze   │
                                    │  - Silver   │
                                    │  - Gold     │
                                    └─────────────┘
```

## 📁 Estrutura do Projeto

```
IPP/
├── config/
│   └── settings.py          # Configurações centralizadas
├── src/
│   ├── rpa/                 # Módulo de automação web
│   │   └── prover_scraper.py
│   ├── storage/             # Upload para GCS
│   │   └── gcs_uploader.py
│   ├── etl/                 # Processamento de dados
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   ├── database/            # Integração BigQuery
│   │   └── bigquery_loader.py
│   └── utils/               # Utilitários
│       ├── logger.py
│       └── exceptions.py
├── tests/                   # Testes automatizados
├── docs/                    # Documentação
│   └── PRD_RPA_PROVER.md
├── data/                    # Dados locais (ignorado no git)
├── logs/                    # Logs de execução
├── main.py                  # Ponto de entrada
├── requirements.txt         # Dependências Python
└── README.md               # Este arquivo
```

## 🚀 Instalação

### Pré-requisitos

- Python 3.9+
- Google Cloud Platform account
- Chrome/Chromium browser

### Passo a Passo

1. **Clone o repositório:**
```bash
git clone <repository-url>
cd IPP
```

2. **Crie um ambiente virtual:**
```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate
```

3. **Instale as dependências:**
```bash
pip install -r requirements.txt
```

4. **Configure as credenciais:**

   a. Crie um arquivo `.env` baseado no `.env.example`:
   ```bash
   cp .env.example .env
   ```

   b. Edite o `.env` com suas credenciais:
   - Credenciais do PROVER
   - ID do projeto GCP
   - Nome do bucket GCS
   
   c. Baixe a service account key do GCP e salve em:
   ```
   config/service-account-key.json
   ```

## 📖 Uso

### Modo Completo (Recomendado)

Executa todo o pipeline: extração → upload → ETL → BigQuery

```bash
python main.py --mode full
```

### Modos Individuais

**Apenas Extração:**
```bash
python main.py --mode extract
```

**Apenas ETL:**
```bash
python main.py --mode etl
```

**Apenas BigQuery:**
```bash
python main.py --mode bigquery
```

**Processar data específica:**
```bash
python main.py --mode etl --date 2025-12-07
```

## 🔄 Pipeline de Dados

### 1. Extração (RPA)
- Login automático no sistema PROVER
- Seleção de instituições
- Download de relatórios financeiros

### 2. Upload (Bronze)
- Upload dos arquivos brutos para GCS
- Estrutura: `gs://p96-ipp/bronze/prover/{instituicao}/{data}/`

### 3. Processamento (Silver)
- Limpeza e padronização dos dados
- Conversão para formato Parquet
- Estrutura: `gs://p96-ipp/silver/prover/{instituicao}/{data}/`

### 4. Transformação (Gold)
- Criação de modelo dimensional
- Tabelas dimensão e fato
- Estrutura: `gs://p96-ipp/gold/prover/{tabela}/{data}/`

### 5. Carga (BigQuery)
- Criação de dataset e tabelas
- Carga dos dados processados
- Tabelas:
  - `dim_instituicao`
  - `fato_movimento_financeiro`

## 📊 Tabelas BigQuery

### dim_instituicao
| Campo | Tipo | Descrição |
|-------|------|-----------|
| id_instituicao | INTEGER | Chave primária |
| nome_instituicao_normalizado | STRING | Nome normalizado |
| nome_instituicao | STRING | Nome original |

### fato_movimento_financeiro
| Campo | Tipo | Descrição |
|-------|------|-----------|
| id_movimento | INTEGER | Chave primária |
| id_instituicao | INTEGER | FK para dim_instituicao |
| data_processamento | TIMESTAMP | Data de processamento |
| data_carga | TIMESTAMP | Data de carga no BQ |
| ... | ... | Outras colunas conforme dados |

## 🔧 Configuração Avançada

### Variáveis de Ambiente

Veja `.env.example` para todas as opções disponíveis.

Principais configurações:
- `HEADLESS_MODE`: Executar navegador em modo headless (true/false)
- `LOG_LEVEL`: Nível de log (DEBUG, INFO, WARNING, ERROR)
- `MAX_RETRIES`: Tentativas em caso de falha
- `BROWSER_TIMEOUT`: Timeout do navegador em segundos

### Personalização

Para adicionar novas instituições, edite:
```python
# config/settings.py
instituicoes: List[str] = [
    "JUNTA MISSIONÁRIA DE PINHEIROS",
    "IGREJA PRESBITERIANA DE PINHEIROS",
    "NOVA INSTITUIÇÃO"  # Adicione aqui
]
```

## 🧪 Testes

```bash
# Executar todos os testes
pytest tests/

# Com cobertura
pytest tests/ --cov=src --cov-report=html
```

## 📝 Logs

Os logs são salvos em:
- Console: saída colorida em tempo real
- Arquivo: `logs/rpa_prover.log` (rotacionado a cada 10MB)

Formato:
```
2025-12-08 19:00:00 | INFO     | module:function:line | Mensagem
```

## 🔐 Segurança

- ✅ Credenciais armazenadas em `.env` (não versionado)
- ✅ Service account keys fora do git
- ✅ Conexões seguras com GCP
- ⚠️ Nunca commite credenciais no código

## 🐛 Troubleshooting

### Erro de autenticação GCP
```
Verifique se a service account key está no local correto:
config/service-account-key.json
```

### ChromeDriver não encontrado
```
O webdriver-manager deve baixar automaticamente.
Se falhar, baixe manualmente em: https://chromedriver.chromium.org/
```

### Timeout do Selenium
```
Aumente BROWSER_TIMEOUT no .env
ou execute sem headless: HEADLESS_MODE=false
```

## 📚 Documentação Adicional

- [PRD Completo](docs/PRD_RPA_PROVER.md)
- Documentação das bibliotecas:
  - [Selenium](https://selenium-python.readthedocs.io/)
  - [Google Cloud Storage](https://cloud.google.com/python/docs/reference/storage/latest)
  - [BigQuery](https://cloud.google.com/python/docs/reference/bigquery/latest)

## 🤝 Contribuindo

1. Crie uma branch: `git checkout -b feature/nova-funcionalidade`
2. Faça commit: `git commit -m 'Adiciona nova funcionalidade'`
3. Push: `git push origin feature/nova-funcionalidade`
4. Abra um Pull Request

## 📄 Licença

[Adicione sua licença aqui]

## 👥 Autores

- Rubens Sousa

## 📞 Suporte

Para questões e suporte, abra uma issue no repositório.

---

Cronob -l 

**Status do Projeto:** 🟢 Ativo

**Última Atualização:** Dezembro 2025










