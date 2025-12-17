"""
Módulo para upload de arquivos para Google Cloud Storage
"""
import os
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account

from config.settings import settings
from src.utils.logger import get_logger
from src.utils.exceptions import UploadException


class GCSUploader:
    """Classe para fazer upload de arquivos para o GCS"""
    
    def __init__(self):
        self.logger = get_logger()
        self.bucket_name = settings.gcs_bucket_name
        self.client = None
        self.bucket = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Inicializa o cliente do Google Cloud Storage"""
        try:
            self.logger.info("Inicializando cliente do Google Cloud Storage...")
            
            # Define as credenciais via variável de ambiente
            if settings.google_application_credentials:
                credentials_path = Path(settings.google_application_credentials)
                if credentials_path.exists():
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path.absolute())
                    self.logger.info(f"Credenciais carregadas de: {credentials_path}")
                else:
                    self.logger.warning(f"Arquivo de credenciais não encontrado: {credentials_path}")
            
            # Cria o cliente
            self.client = storage.Client()
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Verifica se o bucket existe
            if not self.bucket.exists():
                raise UploadException(f"Bucket '{self.bucket_name}' não encontrado no GCS")
            
            self.logger.info(f"✓ Cliente GCS inicializado com sucesso. Bucket: {self.bucket_name}")
            
        except Exception as e:
            self.logger.error(f"Erro ao inicializar cliente GCS: {e}")
            raise UploadException(f"Falha ao inicializar GCS: {e}")
    
    def _mapear_instituicao_para_pasta(self, instituicao: str) -> str:
        """
        Mapeia o nome da instituição para o nome da pasta no GCS
        
        Args:
            instituicao: Nome da instituição
            
        Returns:
            Nome da pasta no GCS
        """
        mapeamento = {
            "JUNTA MISSIONÁRIA DE PINHEIROS": "junta_missionaria",
            "IGREJA PRESBITERIANA DE PINHEIROS": "ipp"
        }
        
        pasta = mapeamento.get(instituicao)
        if not pasta:
            # Se não encontrar no mapeamento, usa um nome sanitizado
            pasta = instituicao.lower().replace(" ", "_").replace("ã", "a").replace("á", "a").replace("é", "e")
            self.logger.warning(f"Instituição '{instituicao}' não encontrada no mapeamento. Usando: {pasta}")
        
        return pasta
    
    def upload_arquivo_bronze(self, arquivo_local: Path, instituicao: str) -> str:
        """
        Faz upload de um arquivo para a camada Bronze do GCS
        
        Args:
            arquivo_local: Path do arquivo local
            instituicao: Nome da instituição
            
        Returns:
            URI do arquivo no GCS (gs://bucket/path)
        """
        try:
            if not arquivo_local.exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {arquivo_local}")
            
            # Mapeia instituição para pasta
            pasta_instituicao = self._mapear_instituicao_para_pasta(instituicao)
            
            # Define o caminho no GCS: bronze/{instituicao}/{nome_arquivo}
            gcs_path = f"bronze/{pasta_instituicao}/{arquivo_local.name}"
            
            self.logger.info(f"Fazendo upload de '{arquivo_local.name}' para '{gcs_path}'...")
            
            # Cria o blob (objeto) no bucket
            blob = self.bucket.blob(gcs_path)
            
            # Define metadados
            blob.metadata = {
                "instituicao": instituicao,
                "data_upload": datetime.now().isoformat(),
                "origem": "RPA PROVER",
                "camada": "bronze"
            }
            
            # Faz o upload
            blob.upload_from_filename(str(arquivo_local))
            
            # URI completa do arquivo
            gcs_uri = f"gs://{self.bucket_name}/{gcs_path}"
            
            # Tamanho do arquivo
            tamanho_mb = arquivo_local.stat().st_size / (1024 * 1024)
            
            self.logger.info(f"✓ Upload concluído: {gcs_uri} ({tamanho_mb:.2f} MB)")
            
            return gcs_uri
            
        except Exception as e:
            self.logger.error(f"Erro ao fazer upload de '{arquivo_local}': {e}")
            raise UploadException(f"Falha no upload: {e}")
    
    def upload_multiplos_arquivos_bronze(self, arquivos_por_instituicao: Dict[str, List[Path]]) -> Dict[str, List[str]]:
        """
        Faz upload de múltiplos arquivos para a camada Bronze
        
        Args:
            arquivos_por_instituicao: Dicionário com instituição e lista de arquivos
            
        Returns:
            Dicionário com instituição e lista de URIs no GCS
        """
        self.logger.info("Iniciando upload para GCS (camada Bronze)...")
        
        resultados = {}
        total_arquivos = sum(len(arquivos) for arquivos in arquivos_por_instituicao.values())
        contador = 0
        
        for instituicao, arquivos in arquivos_por_instituicao.items():
            self.logger.info(f"Processando {len(arquivos)} arquivo(s) da instituição: {instituicao}")
            uris = []
            
            for arquivo in arquivos:
                try:
                    contador += 1
                    self.logger.info(f"[{contador}/{total_arquivos}] Arquivo: {arquivo.name}")
                    
                    # Faz upload do arquivo
                    uri = self.upload_arquivo_bronze(arquivo, instituicao)
                    uris.append(uri)
                    
                except Exception as e:
                    self.logger.error(f"Erro ao processar arquivo '{arquivo}': {e}")
                    # Continua com os próximos arquivos mesmo se um falhar
            
            resultados[instituicao] = uris
            self.logger.info(f"✓ {len(uris)}/{len(arquivos)} arquivo(s) enviado(s) para {instituicao}")
        
        self.logger.info(f"✓ Upload concluído: {contador} arquivo(s) total")
        
        return resultados
