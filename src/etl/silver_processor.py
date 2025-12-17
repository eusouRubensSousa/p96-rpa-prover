"""
Processador da Camada Silver
Consolida dados brutos da camada Bronze em um único arquivo Parquet
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import unicodedata
import numpy as np

from google.cloud import storage
from config.settings import settings
from src.utils.logger import get_logger
from src.utils.exceptions import StorageException

logger = get_logger()


class SilverProcessor:
    """Processa dados da camada Bronze para Silver"""
    
    def __init__(self):
        """Inicializa o processador Silver"""
        self.bucket_name = settings.gcs_bucket_name
        self.client = None
        self.bucket = None
        
    def _initialize_client(self):
        """Inicializa o cliente do GCS"""
        try:
            logger.info("Inicializando cliente do Google Cloud Storage...")
            
            # Usa as credenciais do ambiente
            credentials_path = settings.google_application_credentials
            if credentials_path and Path(credentials_path).exists():
                self.client = storage.Client.from_service_account_json(credentials_path)
            else:
                self.client = storage.Client()
            
            self.bucket = self.client.bucket(self.bucket_name)
            logger.info(f"Cliente inicializado - Bucket: {self.bucket_name}")
            
        except Exception as e:
            raise StorageException(f"Erro ao inicializar cliente GCS: {e}")
    
    def _listar_arquivos_bronze(self) -> Dict[str, str]:
        """
        Lista os arquivos mais recentes de cada pasta Bronze
        
        Returns:
            Dicionário {instituicao: caminho_arquivo_gcs}
        """
        logger.info("Listando arquivos da camada Bronze...")
        
        arquivos_recentes = {}
        
        # Instituições a processar
        instituicoes = {
            "junta_missionaria": "JUNTA MISSIONÁRIA DE PINHEIROS",
            "ipp": "IGREJA PRESBITERIANA DE PINHEIROS"
        }
        
        for pasta, nome_instituicao in instituicoes.items():
            prefix = f"bronze/{pasta}/"
            logger.info(f"Procurando arquivos em: gs://{self.bucket_name}/{prefix}")
            
            # Lista todos os blobs com o prefixo
            blobs = list(self.bucket.list_blobs(prefix=prefix))
            
            # Filtra apenas arquivos CSV
            csv_blobs = [b for b in blobs if b.name.endswith('.csv')]
            
            if not csv_blobs:
                logger.warning(f"Nenhum arquivo CSV encontrado em {prefix}")
                continue
            
            # Ordena por data de modificação (mais recente primeiro)
            csv_blobs.sort(key=lambda x: x.updated, reverse=True)
            arquivo_mais_recente = csv_blobs[0]
            
            arquivos_recentes[nome_instituicao] = arquivo_mais_recente.name
            logger.info(f"✓ {nome_instituicao}: {arquivo_mais_recente.name}")
            logger.info(f"  Data: {arquivo_mais_recente.updated}")
        
        return arquivos_recentes
    
    def _normalizar_texto(self, text):
        """Remove acentos e caracteres especiais"""
        if pd.isna(text):
            return text
        if not isinstance(text, str):
            return text
        # Normaliza para NFKD e remove caracteres não-ASCII
        return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    
    def _baixar_e_processar_csv(self, gcs_path: str, instituicao: str) -> pd.DataFrame:
        """
        Baixa e processa um arquivo CSV do GCS
        
        Args:
            gcs_path: Caminho do arquivo no GCS
            instituicao: Nome da instituição
            
        Returns:
            DataFrame processado
        """
        logger.info(f"Baixando e processando: {gcs_path}")
        
        try:
            # Baixa o arquivo
            blob = self.bucket.blob(gcs_path)
            csv_content = blob.download_as_text(encoding='utf-8')
            
        except UnicodeDecodeError:
            # Tenta com latin-1 se UTF-8 falhar
            logger.warning("Falha com UTF-8, tentando latin-1...")
            blob = self.bucket.blob(gcs_path)
            csv_content = blob.download_as_text(encoding='latin-1')
        
        # Lê o CSV com tratamento de erros
        # O sistema PROVER usa ponto e vírgula como delimitador e aspas duplas
        from io import StringIO
        df = pd.read_csv(
            StringIO(csv_content),
            sep=';',  # O sistema PROVER usa ponto e vírgula
            quotechar='"',  # Aspas duplas ao redor de campos
            on_bad_lines='skip',
            encoding='utf-8',
            engine='python'  # Engine python é mais robusto para CSVs complexos
        )
        
        # Normaliza caracteres especiais em todas as colunas de texto
        logger.info("Normalizando caracteres especiais...")
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(self._normalizar_texto)
        
        # Adiciona coluna de instituição
        df['instituicao'] = instituicao
        
        logger.info(f"✓ Arquivo processado: {len(df)} linhas")
        
        return df
    
    def _consolidar_dataframes(self, arquivos: Dict[str, str]) -> pd.DataFrame:
        """
        Consolida múltiplos DataFrames em um único
        
        Args:
            arquivos: Dicionário {instituicao: caminho_gcs}
            
        Returns:
            DataFrame consolidado
        """
        logger.info("Consolidando dados de todas as instituições...")
        
        dfs = []
        for instituicao, gcs_path in arquivos.items():
            df = self._baixar_e_processar_csv(gcs_path, instituicao)
            dfs.append(df)
        
        # Concatena todos os DataFrames
        df_consolidado = pd.concat(dfs, ignore_index=True)
        
        # Converte NaN para None (NULL)
        df_consolidado = df_consolidado.replace({np.nan: None})
        
        logger.info(f"✓ Consolidação concluída: {len(df_consolidado)} linhas totais")
        
        return df_consolidado
    
    def _salvar_silver(self, df: pd.DataFrame) -> str:
        """
        Salva o DataFrame consolidado na camada Silver
        
        Args:
            df: DataFrame a salvar
            
        Returns:
            Caminho do arquivo salvo no GCS
        """
        logger.info("Salvando dados na camada Silver...")
        
        # Define o caminho de destino (arquivo fixo, será substituído)
        silver_path = "silver/fluxo_caixa_rpa/fluxo_caixa_consolidado.parquet"
        
        # Converte DataFrame para Parquet em memória
        # Padroniza tipos de dados para evitar erros do PyArrow com tipos mistos
        logger.info("Padronizando tipos de dados...")
        for col in df.columns:
            # Converte todas as colunas para string para garantir compatibilidade
            df[col] = df[col].astype(str).replace('nan', '').replace('None', '')
        
        from io import BytesIO
        buffer = BytesIO()
        df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
        buffer.seek(0)
        
        # Upload para o GCS
        blob = self.bucket.blob(silver_path)
        
        # Define metadados
        blob.metadata = {
            'data_processamento': datetime.now().isoformat(),
            'num_linhas': str(len(df)),
            'num_colunas': str(len(df.columns)),
            'instituicoes': ','.join(df['instituicao'].unique())
        }
        
        # Faz o upload (sobrescreve se existir)
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        
        gcs_full_path = f"gs://{self.bucket_name}/{silver_path}"
        logger.info(f"✓ Arquivo salvo: {gcs_full_path}")
        logger.info(f"  Linhas: {len(df):,}")
        logger.info(f"  Colunas: {len(df.columns)}")
        
        return gcs_full_path
    
    def processar(self) -> str:
        """
        Executa o processamento completo da camada Silver
        
        Returns:
            Caminho do arquivo consolidado no GCS
        """
        logger.info("="*80)
        logger.info("INICIANDO PROCESSAMENTO DA CAMADA SILVER")
        logger.info("="*80)
        
        try:
            # Inicializa cliente
            self._initialize_client()
            
            # Lista arquivos Bronze mais recentes
            arquivos = self._listar_arquivos_bronze()
            
            if not arquivos:
                raise StorageException("Nenhum arquivo Bronze encontrado para processar")
            
            # Consolida dados
            df_consolidado = self._consolidar_dataframes(arquivos)
            
            # Salva na camada Silver
            silver_path = self._salvar_silver(df_consolidado)
            
            logger.info("="*80)
            logger.info("✅ PROCESSAMENTO SILVER CONCLUÍDO COM SUCESSO!")
            logger.info("="*80)
            
            return silver_path
            
        except Exception as e:
            logger.error(f"Erro no processamento Silver: {e}")
            raise StorageException(f"Falha no processamento Silver: {e}")

