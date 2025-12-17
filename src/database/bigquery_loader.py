"""
Módulo para carga de dados no BigQuery
"""
from datetime import datetime
from typing import Optional
from src.utils.logger import get_logger


class BigQueryLoader:
    """Carrega dados no BigQuery"""
    
    def __init__(self):
        self.logger = get_logger()
    
    def carregar_tabelas_gold(self, data: Optional[datetime] = None) -> None:
        """
        Carrega tabelas Gold no BigQuery
        
        Args:
            data: Data dos arquivos a processar
        """
        self.logger.info("Iniciando carga no BigQuery...")
        
        # TODO: Implementar carga real no BigQuery
        self.logger.warning("Carga no BigQuery não implementada ainda")
