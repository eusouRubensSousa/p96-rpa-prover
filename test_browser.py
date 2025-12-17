"""
Script de teste para verificar se o navegador está abrindo corretamente
"""
from src.utils.logger import setup_logger, get_logger
from src.rpa.prover_scraper import ProverScraper

if __name__ == "__main__":
    # Setup do logger
    setup_logger()
    logger = get_logger()
    
    logger.info("="*80)
    logger.info("TESTE DE ABERTURA DO NAVEGADOR")
    logger.info("="*80)
    
    try:
        scraper = ProverScraper()
        scraper.driver = scraper._setup_driver()
        scraper.wait = None  # Não precisa para este teste
        
        logger.info("✓ Navegador aberto com sucesso!")
        logger.info("O Chrome deve estar visível na sua tela.")
        logger.info("Aguardando 10 segundos antes de fechar...")
        
        import time
        time.sleep(10)
        
        logger.info("Fechando navegador...")
        scraper.driver.quit()
        logger.info("✓ Teste concluído com sucesso!")
        
    except Exception as e:
        logger.error(f"✗ Erro no teste: {str(e)}")
        import traceback
        traceback.print_exc()
