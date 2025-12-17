"""
Configurações centralizadas do projeto RPA PROVER
Usa Pydantic para validação e carregamento de variáveis de ambiente
"""
from pathlib import Path
from typing import List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configurações do projeto carregadas do .env"""
    
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='ignore'
    )
    
    # ========== Configurações do Projeto ==========
    project_name: str = "RPA PROVER"
    project_version: str = "1.0.0"
    environment: str = Field(default="development", description="Ambiente: development, staging, production")
    
    # ========== Diretórios do Projeto ==========
    base_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent)
    data_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent / "data")
    logs_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent / "logs")
    downloads_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent / "data" / "downloads")
    
    # ========== Credenciais PROVER ==========
    prover_url: str = Field(default="https://sis.sistemaprover.com.br", description="URL do sistema PROVER")
    prover_username: str = Field(default="", description="Usuário do PROVER")
    prover_password: str = Field(default="", description="Senha do PROVER")
    
    # ========== Instituições a Processar ==========
    instituicoes: List[str] = Field(
        default=[
            "JUNTA MISSIONÁRIA DE PINHEIROS",
            "IGREJA PRESBITERIANA DE PINHEIROS"
        ],
        description="Lista de instituições para processar"
    )
    
    # ========== Configurações do Selenium ==========
    headless_mode: bool = Field(default=True, description="Executar navegador em modo headless")
    browser_timeout: int = Field(default=30, description="Timeout do navegador em segundos")
    implicit_wait: int = Field(default=10, description="Espera implícita do Selenium")
    page_load_timeout: int = Field(default=60, description="Timeout de carregamento de página")
    download_timeout: int = Field(default=300, description="Timeout para downloads em segundos")
    
    # ========== Configurações de Retry ==========
    max_retries: int = Field(default=3, description="Número máximo de tentativas")
    retry_delay: int = Field(default=5, description="Delay entre tentativas em segundos")
    
    # ========== Google Cloud Platform ==========
    gcp_project_id: str = Field(default="", description="ID do projeto GCP")
    gcs_bucket_name: str = Field(default="p96-ipp", description="Nome do bucket GCS")
    bigquery_dataset: str = Field(default="prover_data", description="Nome do dataset BigQuery")
    google_application_credentials: Optional[str] = Field(
        default="./config/service-account-key.json",
        description="Path para a service account key JSON"
    )
    
    # ========== Camadas de Dados (Arquitetura Medalhão) ==========
    bronze_prefix: str = Field(default="bronze/prover", description="Prefixo para camada Bronze no GCS")
    silver_prefix: str = Field(default="silver/prover", description="Prefixo para camada Silver no GCS")
    gold_prefix: str = Field(default="gold/prover", description="Prefixo para camada Gold no GCS")
    
    # ========== Configurações de Logging ==========
    log_level: str = Field(default="INFO", description="Nível de log: DEBUG, INFO, WARNING, ERROR, CRITICAL")
    log_format: str = Field(
        default="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | <level>{message}</level>",
        description="Formato do log"
    )
    log_rotation: str = Field(default="10 MB", description="Rotação do arquivo de log")
    log_retention: str = Field(default="30 days", description="Retenção de logs")
    log_compression: str = Field(default="zip", description="Compressão dos logs antigos")
    
    # ========== Configurações de Processamento ==========
    chunk_size: int = Field(default=10000, description="Tamanho do chunk para processamento de dados")
    date_format: str = Field(default="%Y-%m-%d", description="Formato de data padrão")
    datetime_format: str = Field(default="%Y-%m-%d %H:%M:%S", description="Formato de datetime padrão")
    
    # ========== Configurações BigQuery ==========
    bq_location: str = Field(default="US", description="Localização do BigQuery")
    bq_write_disposition: str = Field(default="WRITE_APPEND", description="Disposição de escrita no BQ")
    bq_create_disposition: str = Field(default="CREATE_IF_NEEDED", description="Disposição de criação no BQ")
    
    # ========== Feature Flags ==========
    enable_cleanup: bool = Field(default=True, description="Limpar arquivos temporários após processamento")
    enable_notifications: bool = Field(default=False, description="Enviar notificações de status")
    enable_monitoring: bool = Field(default=False, description="Habilitar monitoramento avançado")
    
    def model_post_init(self, __context) -> None:
        """Cria diretórios necessários após inicialização"""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
    
    @property
    def bronze_path(self) -> str:
        """Caminho completo para camada Bronze"""
        return f"gs://{self.gcs_bucket_name}/{self.bronze_prefix}"
    
    @property
    def silver_path(self) -> str:
        """Caminho completo para camada Silver"""
        return f"gs://{self.gcs_bucket_name}/{self.silver_prefix}"
    
    @property
    def gold_path(self) -> str:
        """Caminho completo para camada Gold"""
        return f"gs://{self.gcs_bucket_name}/{self.gold_prefix}"
    
    @property
    def bigquery_dataset_full(self) -> str:
        """Nome completo do dataset BigQuery"""
        return f"{self.gcp_project_id}.{self.bigquery_dataset}"
    
    def get_instituicao_normalizada(self, nome_instituicao: str) -> str:
        """
        Normaliza o nome da instituição para uso em paths
        
        Args:
            nome_instituicao: Nome original da instituição
            
        Returns:
            Nome normalizado (lowercase, underscores)
        """
        return nome_instituicao.lower().replace(" ", "_").replace("-", "_")
    
    def get_bronze_path_instituicao(self, instituicao: str, data: str) -> str:
        """
        Retorna o path Bronze para uma instituição e data
        
        Args:
            instituicao: Nome da instituição
            data: Data no formato YYYY-MM-DD
            
        Returns:
            Path completo no GCS
        """
        inst_norm = self.get_instituicao_normalizada(instituicao)
        return f"{self.bronze_path}/{inst_norm}/{data}"
    
    def get_silver_path_instituicao(self, instituicao: str, data: str) -> str:
        """
        Retorna o path Silver para uma instituição e data
        
        Args:
            instituicao: Nome da instituição
            data: Data no formato YYYY-MM-DD
            
        Returns:
            Path completo no GCS
        """
        inst_norm = self.get_instituicao_normalizada(instituicao)
        return f"{self.silver_path}/{inst_norm}/{data}"
    
    def get_gold_path_tabela(self, nome_tabela: str, data: str) -> str:
        """
        Retorna o path Gold para uma tabela e data
        
        Args:
            nome_tabela: Nome da tabela
            data: Data no formato YYYY-MM-DD
            
        Returns:
            Path completo no GCS
        """
        return f"{self.gold_path}/{nome_tabela}/{data}"


# Instância global das configurações
settings = Settings()


# Validação básica na importação
if __name__ != "__main__":
    # Apenas avisa se estiver rodando a aplicação (não durante testes)
    import sys
    if "pytest" not in sys.modules:
        if not settings.prover_username or not settings.prover_password:
            import warnings
            warnings.warn(
                "Credenciais do PROVER não configuradas. "
                "Por favor, configure PROVER_USERNAME e PROVER_PASSWORD no arquivo .env"
            )
