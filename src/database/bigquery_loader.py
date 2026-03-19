"""
Módulo para carga de dados no BigQuery
Carrega dados da camada Gold para o BigQuery (Data Warehouse)
"""
import sys
import re
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Optional

# Adiciona o diretório raiz ao path para imports
# Funciona tanto quando executado diretamente quanto quando importado
_root_dir = Path(__file__).resolve().parent.parent.parent
if str(_root_dir) not in sys.path:
    sys.path.insert(0, str(_root_dir))

from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from config.settings import settings
from src.utils.logger import get_logger
from src.utils.exceptions import StorageException

logger = get_logger()

class BigQueryLoader:
    """Carrega dados da camada Gold para o BigQuery"""
    
    def __init__(self):
        """Inicializa o carregador BigQuery"""
        self.bucket_name = settings.gcs_bucket_name
        # Usa valores das settings ou fallback para valores padrão
        self.dataset_id = settings.bigquery_dataset or "P96_IPP"
        self.project_id = settings.gcp_project_id or "lille-422512"
        self.gcs_client = None
        self.bq_client = None
        self.bucket = None
        
    def _initialize_clients(self):
        """Inicializa os clientes do GCS e BigQuery"""
        try:
            logger.info("Inicializando clientes GCS e BigQuery...")
            
            credentials_path = settings.google_application_credentials
            if credentials_path and Path(credentials_path).exists():
                self.gcs_client = storage.Client.from_service_account_json(
                    credentials_path,
                    project=self.project_id
                )
                self.bq_client = bigquery.Client.from_service_account_json(
                    credentials_path,
                    project=self.project_id
                )
            else:
                # Passa o project_id explicitamente
                self.gcs_client = storage.Client(project=self.project_id)
                self.bq_client = bigquery.Client(project=self.project_id)
            
            self.bucket = self.gcs_client.bucket(self.bucket_name)
            
            logger.info(f"✓ Clientes inicializados")
            logger.info(f"  - GCS Bucket: {self.bucket_name}")
            logger.info(f"  - BigQuery: {self.project_id}.{self.dataset_id}")
            
        except Exception as e:
            raise StorageException(f"Erro ao inicializar clientes: {e}")
    
    def _ensure_dataset_exists(self):
        """Garante que o dataset existe no BigQuery"""
        logger.info(f"Verificando dataset {self.dataset_id}...")
        
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        
        try:
            self.bq_client.get_dataset(dataset_ref)
            logger.info(f"✓ Dataset {self.dataset_id} já existe")
        except NotFound:
            logger.info(f"Dataset {self.dataset_id} não encontrado. Criando...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = settings.bq_location
            dataset = self.bq_client.create_dataset(dataset, timeout=30)
            logger.info(f"✓ Dataset {self.dataset_id} criado")
    
    def _extrair_timestamp_do_nome(self, nome_arquivo: str) -> Optional[datetime]:
        """
        Extrai timestamp do nome do arquivo
        Suporta formatos: YYYYMMDD_HHMMSS, YYYYMMDDHHMMSS, etc.
        
        Args:
            nome_arquivo: Nome do arquivo
            
        Returns:
            Datetime extraído ou None se não encontrar
        """
        # Remove extensão e caminho
        nome_base = Path(nome_arquivo).stem
        
        # Tenta padrão YYYYMMDD_HHMMSS ou YYYYMMDDHHMMSS
        padroes = [
            r'(\d{8})_(\d{6})',  # YYYYMMDD_HHMMSS
            r'(\d{14})',          # YYYYMMDDHHMMSS
            r'(\d{8})',           # YYYYMMDD
        ]
        
        for padrao in padroes:
            match = re.search(padrao, nome_base)
            if match:
                try:
                    if len(match.group(0)) == 14:  # YYYYMMDDHHMMSS
                        data_str = match.group(0)
                        return datetime.strptime(data_str, '%Y%m%d%H%M%S')
                    elif len(match.group(0)) == 15 and '_' in match.group(0):  # YYYYMMDD_HHMMSS
                        partes = match.group(0).split('_')
                        return datetime.strptime(f"{partes[0]}{partes[1]}", '%Y%m%d%H%M%S')
                    elif len(match.group(0)) == 8:  # YYYYMMDD
                        return datetime.strptime(match.group(0), '%Y%m%d')
                except ValueError:
                    continue
        
        return None
    
    def _extrair_nome_tabela(self, nome_arquivo: str) -> str:
        """
        Extrai o nome da tabela do nome do arquivo
        Remove timestamp e extensão para identificar o tipo de tabela
        
        Args:
            nome_arquivo: Nome completo do arquivo
            
        Returns:
            Nome da tabela normalizado
        """
        nome_base = Path(nome_arquivo).stem
        
        # Remove timestamp do início (formato YYYYMMDD_HHMMSS ou YYYYMMDDHHMMSS)
        nome_sem_timestamp = re.sub(r'^\d{8}[_\d]{0,7}_?', '', nome_base)
        
        # Remove sufixo _rpa se existir (será adicionado depois)
        nome_tabela = nome_sem_timestamp.replace('_rpa', '').strip('_')
        
        return nome_tabela if nome_tabela else nome_base
    
    def _listar_arquivos_gold(self, data: Optional[datetime] = None) -> List[str]:
        """
        Lista arquivos Parquet da camada Gold e seleciona apenas os mais recentes
        Agrupa por tipo de tabela e seleciona o arquivo mais recente de cada tipo
        
        Args:
            data: Data opcional para filtrar arquivos (não implementado ainda)
            
        Returns:
            Lista de caminhos dos arquivos mais recentes (um por tipo de tabela)
        """
        logger.info("="*80)
        logger.info("LISTAGEM DE ARQUIVOS GOLD - SELEÇÃO DOS MAIS RECENTES")
        logger.info("="*80)
        logger.info("Buscando arquivos com data mais recente por tipo de tabela...")
        logger.info("")
        
        # Busca arquivos no caminho gold/gold_rpa/
        prefix = "gold/gold_rpa/"
        blobs = list(self.bucket.list_blobs(prefix=prefix))
        
        # Filtra apenas arquivos .parquet válidos (não metadata, não vazios)
        parquet_blobs = [
            b for b in blobs 
            if b.name.endswith('.parquet') 
            and not b.name.endswith('metadata.json')
            and b.size and b.size > 0
        ]
        
        if not parquet_blobs:
            logger.warning("⚠ Nenhum arquivo Parquet válido encontrado na camada Gold")
            return []
        
        logger.info(f"📊 Total de arquivos Parquet encontrados: {len(parquet_blobs)}")
        logger.info("   🔄 Carregando metadados dos arquivos...")
        
        # Recarrega blobs para obter metadados completos (time_created)
        for blob in parquet_blobs:
            try:
                blob.reload()
            except Exception as e:
                logger.warning(f"   ⚠ Não foi possível recarregar metadados de {blob.name}: {e}")
        
        # Processa cada arquivo para determinar data e tipo de tabela
        arquivos_com_data = []
        for blob in parquet_blobs:
            # Tenta extrair timestamp do nome do arquivo
            timestamp_nome = self._extrair_timestamp_do_nome(blob.name)
            
            # Usa timestamp do nome se disponível, senão usa time_created, senão usa updated
            if timestamp_nome:
                data_arquivo = timestamp_nome
                fonte_data = "nome do arquivo"
            elif blob.time_created:
                data_arquivo = blob.time_created
                fonte_data = "data de criação"
            else:
                data_arquivo = blob.updated
                fonte_data = "data de modificação"
            
            # Extrai nome da tabela
            nome_tabela = self._extrair_nome_tabela(blob.name)
            
            arquivos_com_data.append({
                'nome': blob.name,
                'nome_tabela': nome_tabela,
                'data': data_arquivo,
                'fonte': fonte_data,
                'tamanho_mb': blob.size / (1024 * 1024) if blob.size else 0
            })
        
        # Agrupa por tipo de tabela e seleciona o mais recente de cada tipo
        tabelas_arquivos = {}
        for arquivo in arquivos_com_data:
            nome_tabela = arquivo['nome_tabela']
            
            # Se não existe ou se este arquivo é mais recente, substitui
            if nome_tabela not in tabelas_arquivos:
                tabelas_arquivos[nome_tabela] = arquivo
            else:
                # Compara datas e mantém o mais recente
                arquivo_atual = tabelas_arquivos[nome_tabela]
                if arquivo['data'] > arquivo_atual['data']:
                    tabelas_arquivos[nome_tabela] = arquivo
        
        # Ordena os arquivos selecionados por data (mais recente primeiro)
        arquivos_selecionados = list(tabelas_arquivos.values())
        arquivos_selecionados.sort(key=lambda x: x['data'], reverse=True)
        
        # Log detalhado
        logger.info("")
        logger.info("📋 ARQUIVOS SELECIONADOS (mais recentes por tipo de tabela):")
        logger.info("-"*80)
        for i, arquivo in enumerate(arquivos_selecionados, 1):
            nome_arquivo = Path(arquivo['nome']).name
            logger.info(f"  {i}. {nome_arquivo}")
            logger.info(f"     📊 Tipo de tabela: {arquivo['nome_tabela']}")
            logger.info(f"     📅 Data ({arquivo['fonte']}): {arquivo['data']}")
            logger.info(f"     💾 Tamanho: {arquivo['tamanho_mb']:.2f} MB")
            logger.info("")
        
        # Log de arquivos que foram ignorados (mais antigos)
        arquivos_ignorados = [
            a for a in arquivos_com_data 
            if a['nome'] not in [s['nome'] for s in arquivos_selecionados]
        ]
        
        if arquivos_ignorados:
            logger.info(f"📋 Arquivos ignorados (versões mais antigas): {len(arquivos_ignorados)}")
            logger.info("   (Apenas os arquivos mais recentes de cada tipo serão processados)")
            logger.info("")
        
        # Retorna apenas os nomes dos arquivos selecionados
        arquivos_ordenados = [arquivo['nome'] for arquivo in arquivos_selecionados]
        
        logger.info("="*80)
        logger.info(f"✅ RESUMO: {len(arquivos_ordenados)} arquivo(s) selecionado(s) para processamento")
        logger.info(f"   (Um arquivo mais recente por tipo de tabela)")
        logger.info("="*80)
        logger.info("")
        
        return arquivos_ordenados
    
    def _baixar_parquet_para_dataframe(self, gcs_path: str) -> pd.DataFrame:
        """Baixa um arquivo Parquet do GCS e retorna como DataFrame"""
        
        blob = self.bucket.blob(gcs_path)
        
        from io import BytesIO
        buffer = BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)
        
        df = pd.read_parquet(buffer)
        return df
    
    def _definir_schema_bigquery(self, df: pd.DataFrame) -> List[bigquery.SchemaField]:
        """Define o schema do BigQuery baseado no DataFrame"""
        
        schema = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            
            # Mapeia tipos do Pandas para BigQuery
            if dtype in ['int64', 'int32', 'Int64', 'Int32']:
                bq_type = 'INT64'
            elif dtype in ['float64', 'float32']:
                bq_type = 'FLOAT64'
            elif dtype == 'bool':
                bq_type = 'BOOLEAN'
            elif dtype.startswith('datetime64'):
                bq_type = 'TIMESTAMP'
            elif dtype == 'object':
                # Verifica se é datetime.date
                if len(df) > 0 and pd.notna(df[col].iloc[0]):
                    first_val = df[col].iloc[0]
                    if isinstance(first_val, datetime):
                        bq_type = 'TIMESTAMP'
                    else:
                        bq_type = 'STRING'
                else:
                    bq_type = 'STRING'
            else:
                bq_type = 'STRING'
            
            schema.append(bigquery.SchemaField(col, bq_type))
        
        return schema
    
    def _carregar_tabela_bigquery(self, df: pd.DataFrame, nome_tabela: str):
        """Carrega DataFrame para uma tabela do BigQuery"""
        
        # Remove sufixo .parquet se houver
        nome_tabela_limpo = nome_tabela.replace('.parquet', '')
        
        # Adiciona sufixo _rpa se não tiver
        if not nome_tabela_limpo.endswith('_rpa'):
            nome_tabela_final = f"{nome_tabela_limpo}_rpa"
        else:
            nome_tabela_final = nome_tabela_limpo
        
        table_id = f"{self.project_id}.{self.dataset_id}.{nome_tabela_final}"
        
        logger.info(f"Carregando {nome_tabela_final}...")
        logger.info(f"  Linhas: {len(df)}")
        
        # Converte datetime.date para datetime para BigQuery
        for col in df.columns:
            if df[col].dtype == 'object' and len(df) > 0:
                first_val = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                if first_val and hasattr(first_val, 'year') and hasattr(first_val, 'month'):
                    # É um objeto de data, converte para datetime
                    df[col] = pd.to_datetime(df[col])
        
        # Define schema
        schema = self._definir_schema_bigquery(df)
        
        # Configuração de load baseada nas settings
        write_disposition_map = {
            'WRITE_APPEND': bigquery.WriteDisposition.WRITE_APPEND,
            'WRITE_TRUNCATE': bigquery.WriteDisposition.WRITE_TRUNCATE,
            'WRITE_EMPTY': bigquery.WriteDisposition.WRITE_EMPTY
        }
        
        create_disposition_map = {
            'CREATE_IF_NEEDED': bigquery.CreateDisposition.CREATE_IF_NEEDED,
            'CREATE_NEVER': bigquery.CreateDisposition.CREATE_NEVER
        }
        
        write_disp = write_disposition_map.get(
            settings.bq_write_disposition, 
            bigquery.WriteDisposition.WRITE_APPEND
        )
        
        create_disp = create_disposition_map.get(
            settings.bq_create_disposition,
            bigquery.CreateDisposition.CREATE_IF_NEEDED
        )
        
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=write_disp,
            create_disposition=create_disp,
            # Permite evolução de schema (ex.: adicionar fk_categoria)
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            ],
        )
        
        # Carrega dados
        job = self.bq_client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        
        # Aguarda conclusão
        job.result()
        
        # Verifica resultado
        table = self.bq_client.get_table(table_id)
        logger.info(f"  ✓ {nome_tabela_final} carregada: {table.num_rows} linhas")
        
        return table_id
    
    def _atualizar_descricoes_tabelas(self, tabelas_carregadas: List[str]):
        """Atualiza descrições das tabelas no BigQuery"""
        logger.info("\nAtualizando descrições das tabelas...")
        
        descricoes = {
            'dim_instituicao_rpa': 'Dimensão de Instituições (Junta Missionária e IPP)',
            'dim_tipo_lancamento_rpa': 'Dimensão de Tipos de Lançamento',
            'dim_categoria_rpa': 'Dimensão de Categorias e Itens de Classificação',
            'dim_conta_rpa': 'Dimensão de Contas Bancárias',
            'dim_centro_custo_rpa': 'Dimensão de Centros de Custo',
            'dim_forma_pagamento_rpa': 'Dimensão de Formas de Pagamento',
            'dim_fornecedor_rpa': 'Dimensão de Fornecedores',
            'dim_tempo_rpa': 'Dimensão Temporal (Data, Ano, Mês, etc.)',
            'fato_fluxo_caixa_rpa': 'Tabela Fato de Fluxo de Caixa - Lançamentos Financeiros'
        }
        
        for table_id in tabelas_carregadas:
            nome_tabela = table_id.split('.')[-1]
            
            if nome_tabela in descricoes:
                try:
                    table_ref = self.bq_client.get_table(table_id)
                    table_ref.description = descricoes[nome_tabela]
                    self.bq_client.update_table(table_ref, ['description'])
                    logger.info(f"  ✓ {nome_tabela}: descrição atualizada")
                except Exception as e:
                    logger.warning(f"  ⚠ Não foi possível atualizar descrição de {nome_tabela}: {e}")
    
    def carregar_tabelas_gold(self, data: Optional[datetime] = None) -> None:
        """
        Carrega tabelas Gold no BigQuery
        
        Args:
            data: Data dos arquivos a processar (opcional)
        """
        logger.info("Iniciando carga no BigQuery...")
        
        try:
            # Inicializa clientes
            self._initialize_clients()
            
            # Garante que dataset existe
            self._ensure_dataset_exists()
            
            # Lista arquivos Gold
            arquivos_gold = self._listar_arquivos_gold(data)
            
            if not arquivos_gold:
                logger.warning("Nenhum arquivo Gold encontrado para carregar")
                logger.info("Verifique se o processamento Silver → Gold foi executado com sucesso")
                return
            
            # Carrega cada arquivo
            logger.info("\nCarregando tabelas no BigQuery...")
            tabelas_carregadas = []
            
            for gcs_path in arquivos_gold:
                # Extrai nome da tabela do caminho
                nome_arquivo = gcs_path.split('/')[-1]
                nome_tabela = nome_arquivo.replace('.parquet', '')
                
                try:
                    # Baixa Parquet
                    df = self._baixar_parquet_para_dataframe(gcs_path)
                    
                    # Carrega no BigQuery
                    table_id = self._carregar_tabela_bigquery(df, nome_tabela)
                    tabelas_carregadas.append(table_id)
                except Exception as e:
                    logger.error(f"Erro ao carregar {nome_tabela}: {e}")
                    logger.exception(e)
                    continue
            
            if tabelas_carregadas:
                # Atualiza descrições
                self._atualizar_descricoes_tabelas(tabelas_carregadas)
                
                logger.info("\n" + "="*80)
                logger.info("✅ CARREGAMENTO BIGQUERY CONCLUÍDO COM SUCESSO!")
                logger.info("="*80)
                logger.info(f"Total de tabelas carregadas: {len(tabelas_carregadas)}")
                logger.info(f"\nDataset: {self.project_id}.{self.dataset_id}")
                logger.info("Tabelas:")
                for table in tabelas_carregadas:
                    logger.info(f"  - {table.split('.')[-1]}")
            else:
                logger.warning("Nenhuma tabela foi carregada com sucesso")
            
        except Exception as e:
            logger.error(f"Erro no carregamento BigQuery: {e}")
            raise StorageException(f"Falha no carregamento BigQuery: {e}")


if __name__ == "__main__":
    """
    Permite executar o script diretamente
    Exemplo: python src/database/bigquery_loader.py
    """
    from src.utils.logger import setup_logger
    
    # Configura o logger antes de usar
    setup_logger()
    
    # Recarrega o logger após setup
    logger = get_logger()
    
    # Cria o loader e executa
    try:
        loader = BigQueryLoader()
        loader.carregar_tabelas_gold()
    except KeyboardInterrupt:
        logger.warning("\nProcesso interrompido pelo usuário (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nErro ao executar carregamento: {e}")
        logger.exception(e)
        sys.exit(1)

