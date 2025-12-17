#!/usr/bin/env python3
"""
Script de Processamento da Camada Gold
Cria modelo dimensional (Star Schema) a partir da camada Silver
"""

import sys
import traceback
from pathlib import Path
from datetime import datetime

# Adiciona o diretório raiz ao path para imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.etl.gold_processor import GoldProcessor
from src.utils.logger import get_logger
from src.utils.exceptions import StorageException

logger = get_logger()


def main():
    """
    Função principal que executa o processamento Gold
    """
    logger.info("="*80)
    logger.info("PROCESSAMENTO DA CAMADA GOLD - MODELO DIMENSIONAL")
    logger.info("="*80)
    logger.info(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80)
    
    try:
        # Criar processador
        processor = GoldProcessor()
        
        # Executar processamento
        caminhos = processor.processar()
        
        # Exibir resumo
        logger.info("\n" + "="*80)
        logger.info("RESUMO DO PROCESSAMENTO GOLD")
        logger.info("="*80)
        logger.info(f"Total de tabelas geradas: {len(caminhos)}")
        logger.info("\nTabelas criadas:")
        for nome, caminho in caminhos.items():
            logger.info(f"  • {nome}")
            logger.info(f"    {caminho}")
        
        logger.info("\n" + "="*80)
        logger.info("✅ MODELO DIMENSIONAL CRIADO COM SUCESSO!")
        logger.info("="*80)
        logger.info("\nPróximos passos:")
        logger.info("  • Carregar dados no BigQuery")
        logger.info("  • Criar visualizações e dashboards")
        logger.info("  • Análise por categoria")
        logger.info("  • Análise por centro de custo")
        logger.info("  • Comparativo entre instituições")
        logger.info("  • Análise de fornecedores (top gastos)")
        logger.info("  • Tendências temporais")
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

