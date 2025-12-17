# Guia de Início Rápido - RPA PROVER

## 🚀 Setup em 5 Minutos

### 1. Pré-requisitos

Certifique-se de ter instalado:
- ✅ Python 3.9 ou superior
- ✅ Google Chrome
- ✅ Git

### 2. Clone e Configure

```bash
# Clone o repositório
git clone <repository-url>
cd IPP

# Crie o ambiente virtual
python -m venv venv

# Ative o ambiente virtual
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Instale as dependências
pip install -r requirements.txt
```

### 3. Configure as Credenciais

**a) Crie o arquivo .env:**

```bash
# Windows
copy .env.example .env
# Linux/Mac
cp .env.example .env
```

**b) Edite o .env com suas informações:**

```env
# Credenciais PROVER
PROVER_USERNAME=seu_usuario
PROVER_PASSWORD=sua_senha

# Google Cloud
GCP_PROJECT_ID=seu-projeto-gcp
GCS_BUCKET_NAME=p96-ipp
BIGQUERY_DATASET=prover_data

# Path da service account key
GOOGLE_APPLICATION_CREDENTIALS=./config/service-account-key.json
```

**c) Baixe a Service Account Key do GCP:**

1. Acesse o [Google Cloud Console](https://console.cloud.google.com/)
2. Vá em: IAM & Admin → Service Accounts
3. Crie ou selecione uma service account
4. Clique em "Keys" → "Add Key" → "Create New Key" → JSON
5. Salve o arquivo como `config/service-account-key.json`

### 4. Permissões Necessárias no GCP

A service account precisa das seguintes roles:
- ✅ Storage Object Admin
- ✅ BigQuery Data Editor
- ✅ BigQuery Job User

### 5. Primeira Execução

**Teste de configuração:**

```bash
python -c "from config.settings import settings; print('✓ Configuração OK')"
```

**Execução em modo de teste (apenas extração):**

```bash
python main.py --mode extract
```

**Execução completa:**

```bash
python main.py --mode full
```

## 📋 Comandos Úteis

### Modos de Execução

```bash
# Pipeline completo (recomendado)
python main.py --mode full

# Apenas extração do PROVER
python main.py --mode extract

# Apenas processamento ETL
python main.py --mode etl

# Apenas carga no BigQuery
python main.py --mode bigquery

# Processar data específica
python main.py --mode etl --date 2025-12-07
```

### Testes

```bash
# Executar todos os testes
pytest tests/

# Com cobertura
pytest tests/ --cov=src

# Apenas testes rápidos
pytest tests/ -m "not slow"
```

### Qualidade de Código

```bash
# Formatar código
black src/ tests/

# Organizar imports
isort src/ tests/

# Verificar estilo
flake8 src/
```

## 🔍 Verificando os Resultados

### 1. Logs

Os logs são salvos em `logs/rpa_prover.log` e também exibidos no console.

```bash
# Ver últimas linhas do log
# Windows PowerShell:
Get-Content logs\rpa_prover.log -Tail 50

# Linux/Mac:
tail -f logs/rpa_prover.log
```

### 2. Arquivos Baixados

Os arquivos baixados ficam em `data/downloads/`

### 3. Google Cloud Storage

Verifique no GCS:
```
gs://p96-ipp/
├── bronze/prover/...
├── silver/prover/...
└── gold/prover/...
```

### 4. BigQuery

Acesse o BigQuery e consulte:

```sql
-- Ver instituições
SELECT * FROM `seu-projeto.prover_data.dim_instituicao`;

-- Ver movimentos financeiros
SELECT * FROM `seu-projeto.prover_data.fato_movimento_financeiro`
LIMIT 100;

-- Estatísticas por instituição
SELECT 
  i.nome_instituicao,
  COUNT(*) as total_movimentos
FROM `seu-projeto.prover_data.fato_movimento_financeiro` f
JOIN `seu-projeto.prover_data.dim_instituicao` i
  ON f.id_instituicao = i.id_instituicao
GROUP BY i.nome_instituicao;
```

## 🐛 Troubleshooting Rápido

### Erro: "ChromeDriver not found"

```bash
# O webdriver-manager deve baixar automaticamente
# Se falhar, instale manualmente:
pip install --upgrade webdriver-manager
```

### Erro: "Permission denied" no GCP

1. Verifique se a service account key está no local correto
2. Confirme as permissões da service account no GCP Console
3. Teste a autenticação:

```bash
python -c "from google.cloud import storage; storage.Client()"
```

### Erro: "Login failed"

1. Verifique as credenciais no `.env`
2. Tente em modo não-headless para ver o que está acontecendo:

```bash
# Edite .env:
HEADLESS_MODE=false
```

### Site do PROVER mudou?

O RPA pode precisar de ajustes nos seletores. Verifique:
- `src/rpa/prover_scraper.py`
- Atualize os seletores CSS/XPath conforme necessário

## 📊 Próximos Passos

Depois de executar com sucesso:

1. **Automatize:** Configure execução automática (cron, Task Scheduler, Cloud Scheduler)
2. **Monitore:** Configure alertas para falhas
3. **Analise:** Crie dashboards com os dados do BigQuery
4. **Expanda:** Adicione novas instituições ou relatórios

## 📚 Documentação Adicional

- [README Completo](../README.md)
- [PRD do Projeto](PRD_RPA_PROVER.md)
- [Arquitetura Detalhada](ARCHITECTURE.md)

## 💡 Dicas

1. **Teste primeiro em modo não-headless** para ver o navegador funcionando
2. **Comece com uma instituição** para validar o processo
3. **Verifique os logs** regularmente para identificar problemas
4. **Faça backups** das configurações antes de mudanças
5. **Use git** para versionar suas customizações

## ❓ Suporte

Encontrou algum problema? 

1. Verifique os logs em `logs/rpa_prover.log`
2. Consulte a seção de Troubleshooting no README
3. Abra uma issue no repositório com:
   - Descrição do erro
   - Logs relevantes
   - Passos para reproduzir

---

**Boa sorte! 🎉**






