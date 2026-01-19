"""
Módulo para processamento Silver → Gold
"""
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

# Adiciona o diretório raiz ao path para permitir execução direta
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.utils.logger import get_logger
from src.etl.gold_processor import GoldProcessor


class SilverToGoldProcessor:
    """Processa dados da camada Silver para Gold"""
    
    def __init__(self):
        self.logger = get_logger()
        self.processor = GoldProcessor()
    
    def processar_silver_to_gold(self, data: Optional[datetime] = None) -> Dict[str, str]:
        """
        Processa dados Silver para Gold
        
        Args:
            data: Data dos arquivos a processar (opcional, não usado atualmente)
            
        Returns:
            Dicionário com nome da tabela e URI no GCS
        """
        self.logger.info("Iniciando processamento Silver → Gold...")
        
        try:
            # Usa o GoldProcessor que já tem toda a implementação
            caminhos = self.processor.processar()
            
            # Retorna o dicionário de caminhos
            return caminhos
            
        except Exception as e:
            self.logger.error(f"Erro no processamento Silver → Gold: {e}")
            raise


if __name__ == "__main__":
    """
    Permite executar o arquivo diretamente para testes
    """
    from src.utils.logger import setup_logger
    
    # Configura o logger
    setup_logger()
    
    # Executa o processamento
    processor = SilverToGoldProcessor()
    resultado = processor.processar_silver_to_gold()
    
    print(f"\n✓ Processamento concluído!")
    print(f"Tabelas Gold geradas: {len(resultado)}")
    for nome_tabela, uri in resultado.items():
        print(f"  - {nome_tabela}: {uri}")




