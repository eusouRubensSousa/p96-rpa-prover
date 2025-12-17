#!/usr/bin/env python3
"""
Script de Carregamento no BigQuery
Carrega dados da camada Gold para o BigQuery (Data Warehouse)
"""

import sys
import traceback
from pathlib import Path
from datetime import datetime

# Adiciona o diretório raiz ao path para imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.etl.bigquery_loader import BigQueryLoader
from src.utils.logger import get_logger
from src.utils.exceptions import StorageException

logger = get_logger()


def main():
    """
    Função principal que executa o carregamento no BigQuery
    """
    logger.info("="*80)
    logger.info("CARREGAMENTO NO BIGQUERY - DATA WAREHOUSE")
    logger.info("="*80)
    logger.info(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80)
    
    try:
        # Criar loader
        loader = BigQueryLoader()
        
        # Executar carregamento
        resultado = loader.carregar()
        
        # Exibir resumo
        logger.info("\n" + "="*80)
        logger.info("RESUMO DO CARREGAMENTO")
        logger.info("="*80)
        logger.info(f"Dataset: {resultado['dataset']}")
        logger.info(f"\nTabelas carregadas ({len(resultado['tabelas'])}):")
        for tabela in resultado['tabelas']:
            logger.info(f"  • {tabela}")
        
        logger.info("\n" + "="*80)
        logger.info("✅ DATA WAREHOUSE ATUALIZADO COM SUCESSO!")
        logger.info("="*80)
        logger.info("\nExemplo de consulta SQL:")
        logger.info("""
  -- Top 10 Fornecedores por Valor
  SELECT 
    f.fornecedor,
    SUM(ft.valor) as total_gasto
  FROM `lille-422512.P96_IPP.fato_fluxo_caixa_rpa` ft
  JOIN `lille-422512.P96_IPP.dim_fornecedor_rpa` f ON ft.sk_fornecedor = f.sk_fornecedor
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 10;
        """)
        logger.info("="*80)
        
    except KeyboardInterrupt:
        logger.warning("\nProcesso interrompido pelo usuário (Ctrl+C)")
        sys.exit(1)
        
    except StorageException as e:
        logger.error(f"\n❌ ERRO NO CARREGAMENTO: {str(e)}")
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

