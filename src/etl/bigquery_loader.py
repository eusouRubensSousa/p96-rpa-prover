"""
Carregador para BigQuery
Carrega dados da camada Gold para o BigQuery (Data Warehouse)
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import time

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
        self.dataset_id = "P96_IPP"
        self.project_id = "lille-422512"
        self.gcs_client = None
        self.bq_client = None
        self.bucket = None
        
    def _initialize_clients(self):
        """Inicializa os clientes do GCS e BigQuery"""
        try:
            logger.info("Inicializando clientes GCS e BigQuery...")
            
            credentials_path = settings.google_application_credentials
            if credentials_path and Path(credentials_path).exists():
                self.gcs_client = storage.Client.from_service_account_json(credentials_path)
                self.bq_client = bigquery.Client.from_service_account_json(credentials_path)
            else:
                self.gcs_client = storage.Client()
                self.bq_client = bigquery.Client()
            
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
            dataset.location = "US"
            dataset = self.bq_client.create_dataset(dataset, timeout=30)
            logger.info(f"✓ Dataset {self.dataset_id} criado")
    
    def _listar_arquivos_gold(self) -> List[str]:
        """Lista arquivos Parquet da camada Gold"""
        logger.info("Listando arquivos da camada Gold...")
        
        prefix = "gold/gold_rpa/"
        blobs = list(self.bucket.list_blobs(prefix=prefix))
        
        # Filtra apenas arquivos .parquet (não metadata)
        parquet_files = [
            b.name for b in blobs 
            if b.name.endswith('.parquet') and not b.name.endswith('metadata.json')
        ]
        
        logger.info(f"✓ Encontrados {len(parquet_files)} arquivos Parquet")
        return parquet_files
    
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
        
        # Adiciona sufixo _rpa
        nome_tabela_final = f"{nome_tabela_limpo}_rpa"
        
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
        
        # Configuração de load
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Sobrescreve
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
            'dim_status_rpa': 'Dimensão de Status de Lançamento',
            'dim_tempo_rpa': 'Dimensão Temporal (Data, Ano, Mês, etc.)',
            'fato_fluxo_caixa_rpa': 'Tabela Fato de Fluxo de Caixa - Lançamentos Financeiros'
        }
        
        for table_id in tabelas_carregadas:
            nome_tabela = table_id.split('.')[-1]
            
            if nome_tabela in descricoes:
                table_ref = self.bq_client.get_table(table_id)
                table_ref.description = descricoes[nome_tabela]
                self.bq_client.update_table(table_ref, ['description'])
                logger.info(f"  ✓ {nome_tabela}: descrição atualizada")
    
    def carregar(self) -> Dict[str, str]:
        """
        Executa o carregamento completo no BigQuery
        
        Returns:
            Dicionário com tabelas carregadas
        """
        logger.info("="*80)
        logger.info("INICIANDO CARREGAMENTO NO BIGQUERY")
        logger.info("="*80)
        
        try:
            # Inicializa clientes
            self._initialize_clients()
            
            # Garante que dataset existe
            self._ensure_dataset_exists()
            
            # Lista arquivos Gold
            arquivos_gold = self._listar_arquivos_gold()
            
            if not arquivos_gold:
                raise StorageException("Nenhum arquivo Gold encontrado para carregar")
            
            # Carrega cada arquivo
            logger.info("\nCarregando tabelas no BigQuery...")
            tabelas_carregadas = []
            
            for gcs_path in arquivos_gold:
                # Extrai nome da tabela do caminho
                nome_arquivo = gcs_path.split('/')[-1]
                nome_tabela = nome_arquivo.replace('.parquet', '')
                
                # Baixa Parquet
                df = self._baixar_parquet_para_dataframe(gcs_path)
                
                # Carrega no BigQuery
                table_id = self._carregar_tabela_bigquery(df, nome_tabela)
                tabelas_carregadas.append(table_id)
            
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
            
            return {
                'dataset': f"{self.project_id}.{self.dataset_id}",
                'tabelas': tabelas_carregadas
            }
            
        except Exception as e:
            logger.error(f"Erro no carregamento BigQuery: {e}")
            raise StorageException(f"Falha no carregamento BigQuery: {e}")

