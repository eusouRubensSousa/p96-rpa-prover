"""
RPA PROVER - Ponto de entrada principal
Script para executar o processo completo de extração, processamento e carga de dados
"""
import sys
import argparse
from datetime import datetime
from pathlib import Path

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings
from src.utils.logger import setup_logger, get_logger
from src.utils.exceptions import ProverRPAException
from src.rpa.prover_scraper import ProverScraper
from src.storage.gcs_uploader import GCSUploader
from src.etl.bronze_to_silver import BronzeToSilverProcessor
from src.etl.silver_to_gold import SilverToGoldProcessor
from src.database.bigquery_loader import BigQueryLoader


def executar_extracao() -> dict:
    """
    Executa a extração de dados do sistema PROVER
    
    Returns:
        Dicionário com instituição e lista de arquivos baixados
    """
    logger = get_logger()
    logger.info("\n" + "="*80)
    logger.info("ETAPA 1: EXTRAÇÃO DE DADOS (RPA)")
    logger.info("="*80 + "\n")
    
    scraper = ProverScraper()
    resultados = scraper.processar_todas_instituicoes()
    
    logger.info("\n" + "="*80)
    logger.info("EXTRAÇÃO CONCLUÍDA")
    logger.info("="*80 + "\n")
    
    return resultados


def executar_upload(arquivos_por_instituicao: dict) -> dict:
    """
    Executa o upload dos arquivos para o GCS (camada Bronze)
    
    Args:
        arquivos_por_instituicao: Dicionário com instituição e arquivos
        
    Returns:
        Dicionário com instituição e URIs no GCS
    """
    logger = get_logger()
    logger.info("\n" + "="*80)
    logger.info("ETAPA 2: UPLOAD PARA GCS (CAMADA BRONZE)")
    logger.info("="*80 + "\n")
    
    uploader = GCSUploader()
    uris_gcs = uploader.upload_multiplos_arquivos_bronze(arquivos_por_instituicao)
    
    logger.info("\n" + "="*80)
    logger.info("UPLOAD CONCLUÍDO")
    logger.info("="*80 + "\n")
    
    return uris_gcs


def executar_processamento_bronze_silver(data: datetime = None) -> list:
    """
    Processa dados da camada Bronze para Silver
    
    Args:
        data: Data dos arquivos a processar
        
    Returns:
        Lista de URIs dos arquivos Silver
    """
    logger = get_logger()
    logger.info("\n" + "="*80)
    logger.info("ETAPA 3: PROCESSAMENTO BRONZE → SILVER")
    logger.info("="*80 + "\n")
    
    processor = BronzeToSilverProcessor()
    silver_uris = processor.processar_todos_bronze(data)
    
    logger.info("\n" + "="*80)
    logger.info("PROCESSAMENTO BRONZE → SILVER CONCLUÍDO")
    logger.info("="*80 + "\n")
    
    return silver_uris


def executar_processamento_silver_gold(data: datetime = None) -> dict:
    """
    Processa dados da camada Silver para Gold
    
    Args:
        data: Data dos arquivos a processar
        
    Returns:
        Dicionário com nome da tabela e URI no GCS
    """
    logger = get_logger()
    logger.info("\n" + "="*80)
    logger.info("ETAPA 4: PROCESSAMENTO SILVER → GOLD")
    logger.info("="*80 + "\n")
    
    processor = SilverToGoldProcessor()
    gold_uris = processor.processar_silver_to_gold(data)
    
    logger.info("\n" + "="*80)
    logger.info("PROCESSAMENTO SILVER → GOLD CONCLUÍDO")
    logger.info("="*80 + "\n")
    
    return gold_uris


def executar_carga_bigquery(data: datetime = None) -> None:
    """
    Carrega dados da camada Gold para o BigQuery
    
    Args:
        data: Data dos arquivos a processar
    """
    logger = get_logger()
    logger.info("\n" + "="*80)
    logger.info("ETAPA 5: CARGA NO BIGQUERY")
    logger.info("="*80 + "\n")
    
    loader = BigQueryLoader()
    loader.carregar_tabelas_gold(data)
    
    logger.info("\n" + "="*80)
    logger.info("CARGA NO BIGQUERY CONCLUÍDA")
    logger.info("="*80 + "\n")


def executar_pipeline_completo() -> None:
    """
    Executa o pipeline completo: Extração → Upload → ETL → BigQuery
    """
    logger = get_logger()
    data_execucao = datetime.now()
    
    logger.info("\n" + "="*80)
    logger.info(f"INICIANDO PIPELINE COMPLETO RPA PROVER")
    logger.info(f"Data/Hora: {data_execucao.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80 + "\n")
    
    try:
        # 1. Extração
        arquivos = executar_extracao()
        
        # 2. Upload para GCS (Bronze)
        uris_bronze = executar_upload(arquivos)
        
        # 3. Processamento Bronze → Silver
        uris_silver = executar_processamento_bronze_silver(data_execucao)
        
        # 4. Processamento Silver → Gold
        uris_gold = executar_processamento_silver_gold(data_execucao)
        
        # 5. Carga no BigQuery
        executar_carga_bigquery(data_execucao)
        
        logger.info("\n" + "="*80)
        logger.info("✓ PIPELINE COMPLETO EXECUTADO COM SUCESSO!")
        logger.info("="*80 + "\n")
        
        # Resumo
        logger.info("RESUMO DA EXECUÇÃO:")
        logger.info(f"- Instituições processadas: {len(arquivos)}")
        logger.info(f"- Arquivos extraídos: {sum(len(arqs) for arqs in arquivos.values())}")
        logger.info(f"- Arquivos Bronze: {sum(len(uris) for uris in uris_bronze.values())}")
        logger.info(f"- Arquivos Silver: {len(uris_silver)}")
        logger.info(f"- Tabelas Gold: {len(uris_gold)}")
        logger.info(f"- Data de execução: {data_execucao.strftime('%Y-%m-%d %H:%M:%S')}")
        
    except ProverRPAException as e:
        logger.error(f"\n{'='*80}")
        logger.error(f"✗ ERRO NO PIPELINE: {str(e)}")
        logger.error(f"{'='*80}\n")
        raise
    
    except Exception as e:
        logger.error(f"\n{'='*80}")
        logger.error(f"✗ ERRO INESPERADO: {str(e)}")
        logger.error(f"{'='*80}\n")
        raise


def main():
    """
    Função principal com CLI
    """
    # Setup do logger
    setup_logger()
    logger = get_logger()
    
    parser = argparse.ArgumentParser(
        description="RPA PROVER - Automação de extração de dados financeiros"
    )
    
    parser.add_argument(
        "--mode",
        choices=["full", "extract", "upload", "etl", "bigquery"],
        default="full",
        help="Modo de execução (default: full)"
    )
    
    parser.add_argument(
        "--date",
        type=str,
        help="Data para processar (formato: YYYY-MM-DD)"
    )
    
    args = parser.parse_args()
    
    # Parse da data se fornecida
    data = None
    if args.date:
        try:
            data = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            logger.error("Formato de data inválido. Use YYYY-MM-DD")
            sys.exit(1)
    
    try:
        logger.info(f"Modo de execução: {args.mode.upper()}")
        
        if args.mode == "full":
            executar_pipeline_completo()
        
        elif args.mode == "extract":
            arquivos = executar_extracao()
            logger.info(f"Arquivos extraídos: {arquivos}")
        
        elif args.mode == "upload":
            logger.info("Modo upload requer arquivos já extraídos")
            logger.info("Execute primeiro com --mode extract")
        
        elif args.mode == "etl":
            silver_uris = executar_processamento_bronze_silver(data)
            gold_uris = executar_processamento_silver_gold(data)
            logger.info(f"ETL concluído - Silver: {len(silver_uris)}, Gold: {len(gold_uris)}")
        
        elif args.mode == "bigquery":
            executar_carga_bigquery(data)
        
        logger.info("\n✓ Execução concluída com sucesso!")
        
    except KeyboardInterrupt:
        logger.warning("\n\nExecução interrompida pelo usuário")
        sys.exit(130)
    
    except Exception as e:
        logger.error(f"\n✗ Erro fatal: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()






