"""
Módulo para processamento Bronze → Silver
"""
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional

# Adiciona o diretório raiz ao path para permitir execução direta
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.utils.logger import get_logger
from src.etl.silver_processor import SilverProcessor


class BronzeToSilverProcessor:
    """Processa dados da camada Bronze para Silver"""
    
    def __init__(self):
        self.logger = get_logger()
        self.processor = SilverProcessor()
    
    def processar_todos_bronze(self, data: Optional[datetime] = None) -> List[str]:
        """
        Processa todos os arquivos Bronze para Silver
        
        Args:
            data: Data dos arquivos a processar (opcional, não usado atualmente)
            
        Returns:
            Lista de URIs dos arquivos Silver
        """
        self.logger.info("Iniciando processamento Bronze → Silver...")
        
        try:
            # Usa o SilverProcessor que já tem toda a implementação
            silver_path = self.processor.processar()
            
            # Retorna como lista de URIs
            return [silver_path]
            
        except Exception as e:
            self.logger.error(f"Erro no processamento Bronze → Silver: {e}")
            raise


if __name__ == "__main__":
    """
    Permite executar o arquivo diretamente para testes
    """
    from src.utils.logger import setup_logger
    
    # Configura o logger
    setup_logger()
    
    # Executa o processamento
    processor = BronzeToSilverProcessor()
    resultado = processor.processar_todos_bronze()
    
    print(f"\n✓ Processamento concluído!")
    print(f"Arquivos Silver gerados: {len(resultado)}")
    for uri in resultado:
        print(f"  - {uri}")

