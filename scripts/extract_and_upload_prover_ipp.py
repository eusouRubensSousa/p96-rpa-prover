#!/usr/bin/env python3
"""
Script de Extração e Upload - IGREJA PRESBITERIANA DE PINHEIROS
Sistema PROVER -> Google Cloud Storage

Este script automatiza:
1. Login no sistema PROVER
2. Seleção da Igreja Presbiteriana de Pinheiros
3. Download dos lançamentos financeiros (CSV)
4. Upload para GCS na pasta bronze/ipp/
5. Limpeza de arquivos temporários
"""

import sys
import traceback
from datetime import datetime
from pathlib import Path

# Adiciona o diretório raiz ao path para imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rpa.prover_scraper import ProverScraper
from src.storage.gcs_uploader import GCSUploader
from src.utils.logger import get_logger
from src.utils.exceptions import ProverRPAException, StorageException
from config.settings import settings

logger = get_logger()


def main():
    """
    Função principal que executa o processo completo de extração e upload para IPP
    """
    logger.info("="*80)
    logger.info("INICIANDO PROCESSO DE EXTRAÇÃO E UPLOAD - IPP")
    logger.info("="*80)
    logger.info(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Bucket GCS: {settings.gcs_bucket_name}")
    logger.info(f"Instituição: IGREJA PRESBITERIANA DE PINHEIROS")
    logger.info("="*80)
    
    try:
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("ETAPA 1: EXTRAÇÃO DE DADOS DO SISTEMA PROVER")
        logger.info("="*80)
        
        # Configurar datas
        data_inicio = "2025-01-01"
        data_fim = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"Período de extração: {data_inicio} até {data_fim}")
        
        # Inicializar scraper
        scraper = ProverScraper()
        # Sobrescrever as instituições apenas para IPP
        scraper.instituicoes = ["IGREJA PRESBITERIANA DE PINHEIROS"]
        
        logger.info("Configurando driver e fazendo login...")
        scraper.setup_driver()
        scraper.login()
        
        logger.info("Selecionando Junta Missionária (padrão) para chegar no dashboard...")
        scraper.selecionar_instituicao("JUNTA MISSIONÁRIA DE PINHEIROS")
        
        logger.info("Agora no dashboard - trocando para Igreja Presbiteriana...")
        scraper.trocar_instituicao_dashboard("IGREJA PRESBITERIANA DE PINHEIROS")
        
        logger.info("Navegando para exportação...")
        scraper.navegar_para_exportacao()
        
        logger.info("Baixando lançamentos financeiros...")
        arquivos = scraper.baixar_lancamentos_financeiros("IGREJA PRESBITERIANA DE PINHEIROS", data_inicio, data_fim)
        
        # Fechar o scraper
        scraper.close()
        
        # Montar resultados no formato esperado
        resultados = {"IGREJA PRESBITERIANA DE PINHEIROS": arquivos}
        
        # Verificar se houve downloads
        total_arquivos = sum(len(arquivos) for arquivos in resultados.values())
        if total_arquivos == 0:
            logger.warning("Nenhum arquivo foi baixado!")
            logger.info("Processo finalizado sem uploads.")
            return
        
        logger.info(f"\n✓ Extração concluída: {total_arquivos} arquivo(s) baixado(s)")
        
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("ETAPA 2: UPLOAD DOS ARQUIVOS PARA O GOOGLE CLOUD STORAGE")
        logger.info("="*80)
        
        # Inicializar uploader
        uploader = GCSUploader()
        
        # Fazer upload dos arquivos
        upload_results = uploader.upload_multiplos_arquivos_bronze(resultados)
        
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("RESUMO DO PROCESSAMENTO")
        logger.info("="*80)
        
        # Exibir resumo detalhado
        for instituicao, arquivos in resultados.items():
            logger.info(f"\n{instituicao}:")
            logger.info(f"  - Arquivos baixados: {len(arquivos)}")
            logger.info(f"  - Uploads bem-sucedidos: {len(upload_results.get(instituicao, []))}")
            
            if upload_results.get(instituicao):
                logger.info(f"  - Arquivos no GCS:")
                for gcs_path in upload_results[instituicao]:
                    logger.info(f"    • {gcs_path}")
        
        logger.info("\n" + "="*80)
        logger.info("✓ PROCESSO CONCLUÍDO COM SUCESSO!")
        logger.info("="*80)
        
    except KeyboardInterrupt:
        logger.warning("\nProcesso interrompido pelo usuário (Ctrl+C)")
        
    except (ProverRPAException, StorageException) as e:
        logger.error(f"\n❌ ERRO DURANTE O PROCESSAMENTO: {str(e)}")
        logger.error("Detalhes do erro:")
        logger.error(traceback.format_exc())
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"\n❌ ERRO INESPERADO: {str(e)}")
        logger.error("Detalhes do erro:")
        logger.error(traceback.format_exc())
        sys.exit(1)
        
    finally:
        # Limpeza de arquivos temporários
        logger.info("\n" + "="*80)
        logger.info("ETAPA 3: LIMPANDO ARQUIVOS LOCAIS TEMPORÁRIOS")
        logger.info("="*80)
        
        try:
            if settings.download_path.exists():
                logger.info(f"Limpando diretório de downloads: {settings.download_path}")
                import shutil
                shutil.rmtree(settings.download_path, ignore_errors=True)
                logger.info("✓ Limpeza concluída.")
        except Exception as e:
            logger.warning(f"Erro na limpeza: {e}")


if __name__ == "__main__":
    main()

