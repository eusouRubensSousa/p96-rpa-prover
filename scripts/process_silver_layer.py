#!/usr/bin/env python3
"""
Script de Processamento da Camada Silver
Consolida dados brutos da camada Bronze em arquivo Parquet
"""

import sys
import traceback
from pathlib import Path
from datetime import datetime

# Adiciona o diretório raiz ao path para imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.etl.silver_processor import SilverProcessor
from src.utils.logger import get_logger
from src.utils.exceptions import StorageException

logger = get_logger()


def main():
    """
    Função principal que executa o processamento Silver
    """
    logger.info("="*80)
    logger.info("PROCESSAMENTO DA CAMADA SILVER")
    logger.info("="*80)
    logger.info(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80)
    
    try:
        # Criar processador
        processor = SilverProcessor()
        
        # Executar processamento
        silver_path = processor.processar()
        
        # Exibir resumo
        logger.info("\n" + "="*80)
        logger.info("RESUMO DO PROCESSAMENTO SILVER")
        logger.info("="*80)
        logger.info(f"Arquivo gerado: {silver_path}")
        
        # Tentar obter metadados do arquivo
        try:
            blob = processor.bucket.blob("silver/fluxo_caixa_rpa/fluxo_caixa_consolidado.parquet")
            if blob.exists():
                blob.reload()
                logger.info(f"\nMetadados:")
                if blob.metadata:
                    for key, value in blob.metadata.items():
                        logger.info(f"  - {key}: {value}")
                logger.info(f"  - Tamanho: {blob.size:,} bytes")
                logger.info(f"  - Data: {blob.updated}")
        except Exception as e:
            logger.warning(f"Não foi possível obter metadados: {e}")
        
        logger.info("\n" + "="*80)
        logger.info("✅ PROCESSAMENTO SILVER CONCLUÍDO!")
        logger.info("="*80)
        
    except KeyboardInterrupt:
        logger.warning("\nProcesso interrompido pelo usuário (Ctrl+C)")
        sys.exit(1)
        
    except StorageException as e:
        logger.error(f"\n❌ ERRO NO PROCESSAMENTO: {str(e)}")
        logger.error("Detalhes do erro:")
        logger.error(traceback.format_exc())
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"\n❌ ERRO INESPERADO: {str(e)}")
        logger.error("Detalhes do erro:")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()





