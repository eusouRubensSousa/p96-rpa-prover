# Objetivo
Criar um modelo dimensional (star schema) para análise de fluxo de caixa e DFC (Demonstração de Fluxo de Caixa) a partir da camada SILVER.

# Contexto
- Arquivo de origem: `C:\Users\RUBENSSOUSA\Downloads\silver_fluxo_caixa_rpa_fluxo_caixa_consolidado.parquet`
- Total de registros: 40.794 lançamentos financeiros
- Período: principalmente 2025 (jan a dez) com alguns registros de 2024
- Instituições: 2 (Igreja Presbiteriana de Pinheiros e Junta Missionária de Pinheiros)
- Tipos de lançamento: Entrada (22.450) e Saída (18.344)

# Estrutura do Modelo Dimensional

## 1. TABELA FATO: `fato_fluxo_caixa`

### Chaves Estrangeiras (SK - Surrogate Keys)
- `sk_instituicao` (INT) - FK para dim_instituicao
- `sk_tipo_lancamento` (INT) - FK para dim_tipo_lancamento
- `sk_categoria` (INT) - FK para dim_categoria
- `sk_conta` (INT) - FK para dim_conta
- `sk_forma_pagamento` (INT) - FK para dim_forma_pagamento
- `sk_origem` (INT) - FK para dim_origem
- `sk_pessoa` (INT) - FK para dim_pessoa (nullable)
- `sk_fornecedor` (INT) - FK para dim_fornecedor (nullable)
- `sk_centro_custo` (INT) - FK para dim_centro_custo (nullable)

### Chaves Degeneradas
- `numero_lancamento` (INT) - Número do lançamento original
- `id_lancamento` (INT) - ID do lançamento original

### Atributos Temporais
- `data` (DATE) - Data do lançamento
- `ano` (INT) - Ano extraído da data
- `trimestre` (INT) - Trimestre (1-4)

### Métricas (Fatos)
- `valor` (DECIMAL(18,2)) - Valor do lançamento (positivo para entrada, negativo para saída)

### Atributos Descritivos (da Fato)
- `numero_documento` (VARCHAR(50)) - Número do documento (nullable)
- `historico` (TEXT) - Histórico do lançamento
- `observacao` (TEXT) - Observação do lançamento
- `conferido` (VARCHAR(10)) - Status de conferência ("Sim", "Não", etc.)
- `conferido_por` (VARCHAR(100)) - Quem conferiu
- `data_conferencia` (DATE) - Data da conferência (nullable)

### Campos de Auditoria
- `id_pessoa_realizou_lancamento` (INT) - ID de quem realizou o lançamento
- `pessoa_realizou_lancamento` (VARCHAR(200)) - Nome de quem realizou
- `data_criacao` (TIMESTAMP) - Data de criação do lançamento
- `id_pessoa_ultima_alteracao` (INT) - ID de quem alterou (nullable)
- `pessoa_ultima_alteracao` (VARCHAR(200)) - Nome de quem alterou
- `data_ultima_alteracao` (TIMESTAMP) - Data da última alteração

### Informações de Documentos Específicos (nullable)
- `sequencial_remessa_boleto` (INT)
- `data_vencimento_boleto` (DATE)
- `observacao_cheque` (VARCHAR(500))
- `numero_cheque` (VARCHAR(50))
- `agencia_cheque` (VARCHAR(20))
- `conta_cheque` (VARCHAR(50))

---

## 2. DIMENSÃO: `dim_instituicao`

### Estrutura
- `sk_instituicao` (INT, PK) - Surrogate Key
- `nome_instituicao` (VARCHAR(200)) - Nome da instituição
- `tipo_instituicao` (VARCHAR(50)) - "Igreja" ou "Junta Missionária"
- `data_cadastro` (TIMESTAMP) - Data de cadastro na dimensão

---

## 3. DIMENSÃO: `dim_tipo_lancamento`

### Estrutura
- `sk_tipo_lancamento` (INT, PK) - Surrogate Key
- `tipo` (VARCHAR(20)) - "Entrada" ou "Saída"
- `sinal` (INT) - +1 para Entrada, -1 para Saída
- `descricao` (VARCHAR(100)) - Descrição do tipo

---

## 4. DIMENSÃO: `dim_categoria` (SCD Tipo 1 com Hierarquia)

### Estrutura
- `sk_categoria` (INT, PK) - Surrogate Key
- `classificacao_categoria` (VARCHAR(20)) - Código da classificação (ex: "1.1.01")
- `nome_categoria` (VARCHAR(200)) - Nome da categoria
- `nome_item` (VARCHAR(200)) - Nome do item
- `classificacao_item` (VARCHAR(20)) - Código de classificação do item (nullable)
- `nivel_1` (VARCHAR(10)) - Primeiro nível da hierarquia (ex: "1")
- `nivel_2` (VARCHAR(10)) - Segundo nível (ex: "1.1")
- `nivel_3` (VARCHAR(10)) - Terceiro nível (ex: "1.1.01")
- `tipo_categoria` (VARCHAR(50)) - "Receita", "Despesa", "Investimento", etc.
- `ativo` (BOOLEAN) - Se está ativo

---

## 5. DIMENSÃO: `dim_conta` (SCD Tipo 2)

### Estrutura
- `sk_conta` (INT, PK) - Surrogate Key
- `nome_conta` (VARCHAR(100)) - Nome da conta
- `numero_conta` (VARCHAR(50)) - Número da conta extraído
- `banco` (VARCHAR(50)) - Nome do banco
- `tipo_conta` (VARCHAR(30)) - "Corrente", "Poupança", "Integração", etc.
- `valido_de` (DATE) - Data de início de validade (SCD2)
- `valido_ate` (DATE) - Data de fim de validade (SCD2, NULL se atual)
- `ativo` (BOOLEAN) - Se está ativo atualmente

---

## 6. DIMENSÃO: `dim_forma_pagamento`

### Estrutura
- `sk_forma_pagamento` (INT, PK) - Surrogate Key
- `forma_pagamento` (VARCHAR(50)) - Nome da forma de pagamento
- `categoria_forma` (VARCHAR(30)) - "Digital", "Cartão", "Transferência", "Outros"
- `eh_eletronico` (BOOLEAN) - Se é meio eletrônico
- `ordem_exibicao` (INT) - Ordem para relatórios

---

## 7. DIMENSÃO: `dim_origem`

### Estrutura
- `sk_origem` (INT, PK) - Surrogate Key
- `id_origem` (INT) - ID da origem (nullable)
- `nome_origem` (VARCHAR(200)) - Nome da origem
- `tipo_origem` (VARCHAR(50)) - "Conta a Pagar", "Conta a Receber", "Plano Contribuição", "Lançamento Avulso"
- `ativo` (BOOLEAN) - Se está ativo

---

## 8. DIMENSÃO: `dim_pessoa` (SCD Tipo 1)

### Estrutura
- `sk_pessoa` (INT, PK) - Surrogate Key
- `id_pessoa` (INT) - ID original da pessoa (nullable)
- `nome_pessoa` (VARCHAR(200)) - Nome da pessoa
- `cpf` (VARCHAR(14)) - CPF formatado (nullable)
- `tipo_pessoa` (VARCHAR(30)) - "Membro", "Colaborador", "Visitante", etc.
- `ativo` (BOOLEAN) - Se está ativo

---

## 9. DIMENSÃO: `dim_fornecedor` (SCD Tipo 1)

### Estrutura
- `sk_fornecedor` (INT, PK) - Surrogate Key
- `id_fornecedor` (INT) - ID original do fornecedor (nullable)
- `nome_fornecedor` (VARCHAR(200)) - Nome do fornecedor
- `cpf_cnpj` (VARCHAR(18)) - CPF ou CNPJ formatado (nullable)
- `tipo_documento` (VARCHAR(10)) - "CPF" ou "CNPJ"
- `ativo` (BOOLEAN) - Se está ativo

---

## 10. DIMENSÃO: `dim_centro_custo`

### Estrutura
- `sk_centro_custo` (INT, PK) - Surrogate Key
- `nome_centro_custo` (VARCHAR(100)) - Nome do centro de custo
- `tipo_centro_custo` (VARCHAR(50)) - Categorização do centro de custo
- `nivel` (INT) - Nível hierárquico
- `ativo` (BOOLEAN) - Se está ativo

---

# Requisitos de Implementação

## Tecnologia
- Linguagem: Python 3.11+
- Biblioteca principal: Pandas para processamento
- Formato de saída: Parquet (camada GOLD)
- Incluir validações de qualidade de dados

## Estrutura de Código
Criar um script modular com:

1. **Função principal** `create_gold_tables()`
2. **Funções para cada dimensão** (ex: `create_dim_categoria()`, `create_dim_instituicao()`, etc.)
3. **Função para criar a fato** `create_fato_fluxo_caixa()`
4. **Funções de validação** para verificar:
   - Completude dos dados
   - Integridade referencial
   - Valores nulos onde não deveriam existir
   - Duplicatas
5. **Função de geração de métricas** para criar arquivo de log com:
   - Total de registros por tabela
   - Data de processamento
   - Estatísticas básicas

## Regras de Negócio

### Tratamento de Valores Nulos
- Se `Pessoa Lançamento` for nulo ou "nan", criar registro "Não Informado" em dim_pessoa
- Se `Fornecedor` for nulo ou "nan", criar registro "Não Informado" em dim_fornecedor
- Se `Centro Custo` for nulo ou "nan", criar registro "Não Alocado"
- Se `Origem` for nulo ou "nan", usar "Lançamento Avulso"

### Categorização Automática
- Extrair banco da conta (ex: "BRADESCO CC 144.904-4" → banco="BRADESCO", tipo="CC")
- Classificar forma de pagamento em categorias (Digital: PIX, TED, etc.)
- Derivar tipo_categoria da classificação (1.x.x = Receita, 2.x.x = Despesa, 3.x.x = Projetos, etc.)

### Hierarquia de Categoria
- Extrair níveis da classificação (ex: "1.1.01" → nivel_1="1", nivel_2="1.1", nivel_3="1.1.01")

### Atributos Temporais na Fato
- Extrair ano e trimestre da coluna `data`

### Valores
- Manter valores com sinal correto (Entrada positivo, Saída negativo)
- Arredondar para 2 casas decimais

## Estrutura de Pastas (Sugerida)
```
/gold
  ├── fato_fluxo_caixa.parquet
  ├── dim_instituicao.parquet
  ├── dim_tipo_lancamento.parquet
  ├── dim_categoria.parquet
  ├── dim_conta.parquet
  ├── dim_forma_pagamento.parquet
  ├── dim_origem.parquet
  ├── dim_pessoa.parquet
  ├── dim_fornecedor.parquet
  ├── dim_centro_custo.parquet
  └── metadata.json (com métricas e log de processamento)
```

## Validações de Qualidade

1. **Integridade Referencial**: Todas as FKs da fato devem existir nas dimensões
2. **Completude**: Campos obrigatórios não devem ter nulos
3. **Unicidade**: PKs devem ser únicas
4. **Consistência de Valores**:
   - Valor total Entrada deve bater com soma de lançamentos Entrada
   - Valor total Saída deve bater com soma de lançamentos Saída
5. **Range de Datas**: Todas as datas devem estar entre min e max do dataset

## Output Esperado

O script deve:
1. Ler o arquivo parquet da camada SILVER
2. Criar todas as 10 tabelas (1 fato + 9 dimensões)
3. Salvar em formato parquet otimizado na pasta GOLD
4. Gerar relatório de validação em JSON com:
   - Total de registros por tabela
   - Data e hora de processamento
   - Resultado das validações (PASS/FAIL)
   - Estatísticas descritivas
5. Exibir resumo no console

## Exemplo de Uso Final (Queries Analíticas Esperadas)

O modelo deve permitir queries como:
- Fluxo de caixa mensal por categoria
- DFC por método direto (receitas - despesas por categoria)
- DFC por método indireto
- Análise por centro de custo
- Análise por forma de pagamento
- Comparativo entre instituições
- Análise de fornecedores (top gastos)
- Tendências temporais

---

**Importante**:
- Usar boas práticas de código (type hints, docstrings, logging)
- Criar funções reutilizáveis
- Documentar decisões de modelagem
- Incluir tratamento de erros
- Gerar logs detalhados do processamento
