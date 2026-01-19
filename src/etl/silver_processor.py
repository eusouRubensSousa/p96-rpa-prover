"""
Processador da Camada Silver
Consolida dados brutos da camada Bronze em um único arquivo Parquet
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import unicodedata
import numpy as np

# Adiciona o diretório raiz ao path para permitir execução direta
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

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
            
            # Obtém project_id das configurações ou usa fallback
            project_id = settings.gcp_project_id or "lille-422512"
            
            # Usa as credenciais do ambiente
            credentials_path = settings.google_application_credentials
            if credentials_path and Path(credentials_path).exists():
                self.client = storage.Client.from_service_account_json(
                    credentials_path,
                    project=project_id
                )
            else:
                # Passa o project_id explicitamente
                self.client = storage.Client(project=project_id)
            
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Verifica se o bucket existe
            try:
                if not self.bucket.exists():
                    raise StorageException(
                        f"Bucket '{self.bucket_name}' não existe no projeto '{project_id}'. "
                        f"Verifique se o nome do bucket está correto nas configurações (GCS_BUCKET_NAME) "
                        f"ou se o bucket foi criado no projeto GCP."
                    )
            except Exception as e:
                if "does not exist" in str(e) or "404" in str(e):
                    raise StorageException(
                        f"Bucket '{self.bucket_name}' não encontrado no projeto '{project_id}'. "
                        f"Verifique:\n"
                        f"  1. Se o bucket existe no projeto GCP\n"
                        f"  2. Se o nome do bucket está correto em config/settings.py ou arquivo .env (GCS_BUCKET_NAME)\n"
                        f"  3. Se as credenciais têm permissão para acessar o bucket"
                    )
                raise
            
            logger.info(f"Cliente inicializado - Bucket: {self.bucket_name}, Project: {project_id}")
            
        except StorageException:
            raise
        except Exception as e:
            raise StorageException(f"Erro ao inicializar cliente GCS: {e}")
    
    def _listar_arquivos_bronze(self) -> Dict[str, str]:
        """
        Lista os arquivos mais recentes de cada pasta Bronze
        Seleciona o arquivo com data de criação mais recente em cada pasta
        Garante que apenas arquivos CSV válidos sejam processados
        
        Returns:
            Dicionário {instituicao: caminho_arquivo_gcs}
        """
        logger.info("="*80)
        logger.info("LISTAGEM DE ARQUIVOS BRONZE - SELEÇÃO DOS MAIS RECENTES")
        logger.info("="*80)
        logger.info("Buscando arquivos com data de criação mais recente em cada pasta...")
        logger.info("")
        
        arquivos_recentes = {}
        
        # Instituições a processar
        instituicoes = {
            "junta_missionaria": "JUNTA MISSIONÁRIA DE PINHEIROS",
            "ipp": "IGREJA PRESBITERIANA DE PINHEIROS"
        }
        
        for pasta, nome_instituicao in instituicoes.items():
            prefix = f"bronze/{pasta}/"
            logger.info(f"📁 PASTA: {prefix}")
            logger.info(f"   Instituição: {nome_instituicao}")
            logger.info("-"*80)
            
            try:
                # Lista todos os blobs com o prefixo
                blobs = list(self.bucket.list_blobs(prefix=prefix))
                
                # Filtra apenas arquivos CSV válidos (ignora pastas e outros arquivos)
                csv_blobs = [
                    b for b in blobs 
                    if b.name.endswith('.csv') and b.size and b.size > 0
                ]
                
                if not csv_blobs:
                    logger.warning(f"⚠ Nenhum arquivo CSV válido encontrado em {prefix}")
                    logger.info("")
                    continue
                
                logger.info(f"   📊 Total de arquivos CSV encontrados: {len(csv_blobs)}")
                
                # Recarrega os blobs para obter metadados completos (time_created)
                logger.info("   🔄 Carregando metadados dos arquivos...")
                for blob in csv_blobs:
                    try:
                        blob.reload()
                    except Exception as e:
                        logger.warning(f"   ⚠ Erro ao recarregar {blob.name}: {e}")
                
                # Ordena por data de criação (time_created) - mais recente primeiro
                # Prioridade: 1) time_created, 2) updated como fallback
                csv_blobs.sort(
                    key=lambda x: x.time_created if x.time_created else x.updated, 
                    reverse=True
                )
                
                # Seleciona o arquivo mais recente
                arquivo_mais_recente = csv_blobs[0]
                
                # Log detalhado do arquivo selecionado
                nome_arquivo = Path(arquivo_mais_recente.name).name
                data_criacao = arquivo_mais_recente.time_created or arquivo_mais_recente.updated
                tamanho_mb = arquivo_mais_recente.size / (1024 * 1024) if arquivo_mais_recente.size else 0
                fonte_data = "criação" if arquivo_mais_recente.time_created else "modificação"
                
                arquivos_recentes[nome_instituicao] = arquivo_mais_recente.name
                
                logger.info(f"   ✅ ARQUIVO SELECIONADO (mais recente):")
                logger.info(f"      📄 Nome: {nome_arquivo}")
                logger.info(f"      📅 Data de {fonte_data}: {data_criacao}")
                logger.info(f"      📊 Tamanho: {tamanho_mb:.2f} MB ({arquivo_mais_recente.size:,} bytes)")
                
                # Log dos outros arquivos encontrados (para validação)
                if len(csv_blobs) > 1:
                    logger.info(f"   📋 Comparação com outros arquivos ({len(csv_blobs)} total):")
                    for i, blob in enumerate(csv_blobs[:3], 1):  # Mostra até 3 arquivos mais recentes
                        nome = Path(blob.name).name
                        data = blob.time_created or blob.updated
                        tamanho = (blob.size / (1024 * 1024)) if blob.size else 0
                        marcador = "👉" if i == 1 else "  "
                        logger.info(f"      {marcador} {i}. {nome}")
                        logger.info(f"         📅 {data}")
                        logger.info(f"         📊 {tamanho:.2f} MB")
                    if len(csv_blobs) > 3:
                        logger.info(f"      ... e mais {len(csv_blobs) - 3} arquivo(s) mais antigo(s)")
                
                logger.info("")
                
            except Exception as e:
                error_msg = str(e)
                if "does not exist" in error_msg or "404" in error_msg or "not found" in error_msg.lower():
                    project_id = settings.gcp_project_id or "lille-422512"
                    raise StorageException(
                        f"Bucket '{self.bucket_name}' não encontrado no projeto '{project_id}'. "
                        f"\n\nVerifique:\n"
                        f"  1. Se o bucket existe no projeto GCP\n"
                        f"  2. Se o nome do bucket está correto em config/settings.py ou arquivo .env (GCS_BUCKET_NAME)\n"
                        f"  3. Se as credenciais têm permissão para acessar o bucket\n"
                        f"  4. Se o projeto GCP está correto (atual: {project_id})"
                    )
                raise
        
        # Resumo final
        logger.info("="*80)
        if not arquivos_recentes:
            logger.warning("⚠ Nenhum arquivo Bronze encontrado para processar")
            logger.warning("   Verifique se há arquivos CSV válidos nas pastas Bronze")
        else:
            logger.info("📊 RESUMO: ARQUIVOS SELECIONADOS PARA PROCESSAMENTO")
            logger.info("="*80)
            for instituicao, caminho_arquivo in arquivos_recentes.items():
                nome_arquivo = Path(caminho_arquivo).name
                logger.info(f"✅ {instituicao}:")
                logger.info(f"   📄 {nome_arquivo}")
            logger.info("")
            logger.info(f"✓ Total de {len(arquivos_recentes)} arquivo(s) selecionado(s)")
            logger.info("💡 Estes arquivos serão processados e aparecerão nas tabelas RPA do BigQuery")
        logger.info("="*80)
        logger.info("")
        
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

