"""
Testes para o módulo ProverScraper
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from src.rpa.prover_scraper import ProverScraper
from src.utils.exceptions import LoginException, NavigationException


class TestProverScraper:
    """Testes para a classe ProverScraper"""
    
    @pytest.fixture
    def scraper(self):
        """Fixture que retorna uma instância do scraper"""
        return ProverScraper()
    
    def test_inicializacao(self, scraper):
        """Testa inicialização do scraper"""
        assert scraper.driver is None
        assert scraper.wait is None
        assert scraper.download_path is not None
        assert len(scraper.instituicoes) > 0
    
    @patch('src.rpa.prover_scraper.webdriver.Chrome')
    @patch('src.rpa.prover_scraper.ChromeDriverManager')
    def test_setup_driver(self, mock_driver_manager, mock_chrome, scraper):
        """Testa configuração do driver"""
        mock_chrome.return_value = MagicMock()
        
        scraper.setup_driver()
        
        assert scraper.driver is not None
        assert scraper.wait is not None
    
    @patch('src.rpa.prover_scraper.webdriver.Chrome')
    def test_login_sucesso(self, mock_chrome, scraper):
        """Testa login bem-sucedido"""
        # Mock do driver
        mock_driver = MagicMock()
        mock_driver.current_url = "https://sis.sistemaprover.com.br/home"
        scraper.driver = mock_driver
        scraper.wait = MagicMock()
        
        # Mock dos elementos
        mock_username = MagicMock()
        mock_password = MagicMock()
        mock_button = MagicMock()
        
        scraper.wait.until.return_value = mock_username
        mock_driver.find_element.side_effect = [mock_password, mock_button]
        
        # Executa login
        scraper.login()
        
        # Verifica chamadas
        mock_username.send_keys.assert_called_once()
        mock_password.send_keys.assert_called_once()
        mock_button.click.assert_called_once()
    
    def test_normalizar_nome_instituicao(self, scraper):
        """Testa normalização de nomes"""
        # Importa o método de normalização do GCSUploader
        from src.storage.gcs_uploader import GCSUploader
        
        uploader = GCSUploader()
        
        assert uploader._normalizar_nome_instituicao(
            "JUNTA MISSIONÁRIA DE PINHEIROS"
        ) == "junta_missionaria_de_pinheiros"


# Exemplo de como executar:
# pytest tests/test_prover_scraper.py -v










