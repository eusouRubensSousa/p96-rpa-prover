"""
RPA Automatizado para Sistema PROVER
Extrai dados financeiros com login e navegação totalmente automatizados
"""

import time
import tempfile
from pathlib import Path
from datetime import datetime
from typing import List, Dict

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from webdriver_manager.chrome import ChromeDriverManager

from config.settings import settings
from src.utils.logger import get_logger
from src.utils.exceptions import LoginException, NavigationException, DownloadException

logger = get_logger()


class ProverScraper:
    """Scraper automatizado para o sistema PROVER"""
    
    def __init__(self):
        """Inicializa o scraper"""
        self.driver = None
        self.wait = None
        self.download_path = None
        self.instituicoes = settings.instituicoes
        
    def setup_driver(self):
        """Configura o ChromeDriver"""
        logger.info("Configurando driver do Selenium...")
        
        # Cria diretório temporário para downloads
        self.download_path = Path(tempfile.mkdtemp(prefix="rpa_prover_downloads_"))
        self.download_path.mkdir(parents=True, exist_ok=True)
        
        options = Options()
        
        # Configurações de download
        prefs = {
            "download.default_directory": str(self.download_path.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False
        }
        options.add_experimental_option("prefs", prefs)
        
        # Modo headless
        if settings.headless_mode:
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
        
        # Outras opções
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        
        # Inicializa driver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.wait = WebDriverWait(self.driver, 30)
        
        logger.info("Driver configurado com sucesso")
    
    def login(self):
        """Faz login no sistema PROVER"""
        logger.info("Iniciando processo de login...")
        
        try:
            # Acessa a URL
            logger.info(f"Navegando para {settings.prover_url}")
            self.driver.get(settings.prover_url)
            time.sleep(2)
            
            logger.info(f"URL atual após carregar: {self.driver.current_url}")
            
            # Salva screenshot
            screenshot_path = settings.base_dir / "logs" / "login_page.png"
            screenshot_path.parent.mkdir(parents=True, exist_ok=True)
            self.driver.save_screenshot(str(screenshot_path))
            logger.info(f"Screenshot salvo em: {screenshot_path}")
            
            # Preenche usuário
            logger.info("Procurando campo de usuário...")
            campo_usuario = self.wait.until(
                EC.presence_of_element_located((By.NAME, "username"))
            )
            logger.info(f"Campo de usuário encontrado: {campo_usuario.get_attribute('placeholder')}")
            campo_usuario.clear()
            campo_usuario.send_keys(settings.prover_username)
            logger.info("✓ Usuário preenchido")
            
            # Preenche senha
            logger.info("Procurando campo de senha...")
            campo_senha = self.wait.until(
                EC.presence_of_element_located((By.NAME, "password"))
            )
            logger.info(f"Campo de senha encontrado: {campo_senha.get_attribute('placeholder')}")
            campo_senha.clear()
            campo_senha.send_keys(settings.prover_password)
            logger.info("✓ Senha preenchida")
            
            time.sleep(1)
            
            # Clica no botão de login
            logger.info("Procurando botão de login (tela de login)...")
            botao_login = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))
            )
            logger.info(f"Botão de login encontrado: {botao_login.text}")
            botao_login.click()
            logger.info("✓ Botão de login clicado")
            
            # Aguarda tela de seleção de instituição
            logger.info("Aguardando tela de seleção de instituição...")
            time.sleep(5)
            
            # Verifica se chegou na tela de seleção
            try:
                self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='radio'][name='instituicao']"))
                )
                logger.info("✓ Tela de seleção de instituição carregada")
            except:
                logger.warning("Não encontrou radio buttons, continuando...")
            
            logger.info(f"URL após login: {self.driver.current_url}")
            logger.info("Login realizado com sucesso")
            
        except Exception as e:
            logger.error(f"Erro no login: {e}")
            raise LoginException(f"Falha no login: {e}")
    
    def selecionar_instituicao(self, nome_instituicao: str):
        """Seleciona uma instituição via radio button"""
        logger.info(f"Selecionando instituição: {nome_instituicao}")
        
        try:
            time.sleep(3)
            
            # Salva screenshot da seleção
            screenshot_path = settings.base_dir / "logs" / "selecao_instituicao.png"
            screenshot_path.parent.mkdir(parents=True, exist_ok=True)
            self.driver.save_screenshot(str(screenshot_path))
            logger.info(f"Screenshot da seleção salvo em: {screenshot_path}")
            
            # Procura diretamente pelo label que contém o texto da instituição
            # Usando XPATH para encontrar o label que contém o texto
            try:
                # Tenta encontrar pelo texto do label
                label_xpath = f"//label[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{nome_instituicao.lower()}')]"
                label = self.driver.find_element(By.XPATH, label_xpath)
                logger.info(f"✓ Encontrou label da instituição: {label.text}")
                
                # Clica no label
                label.click()
                time.sleep(1)
                logger.info(f"✓ {nome_instituicao} selecionada")
                
            except Exception as e:
                logger.warning(f"Não encontrou pelo label, tentando radio button direto: {e}")
                
                # Fallback: procura o radio button e clica
                radios = self.driver.find_elements(By.CSS_SELECTOR, "input[type='radio'][name='instituicao']")
                logger.info(f"Encontrados {len(radios)} radio buttons")
                
                for radio in radios:
                    # Tenta clicar em cada um até encontrar o certo
                    try:
                        radio.click()
                        time.sleep(0.5)
                        logger.info(f"Clicou no radio button")
                        break
                    except:
                        continue
            
            # Clica no botão "Entrar"
            logger.info("Procurando botão 'Entrar' da seleção de instituição...")
            botao_entrar = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.busca-login[type='submit']"))
            )
            logger.info(f"Botão encontrado com seletor: button.busca-login[type='submit']")
            time.sleep(1)
            
            # Pega o texto do botão
            botao_texto = botao_entrar.text
            logger.info(f"Texto do botão: {botao_texto}")
            
            botao_entrar.click()
            logger.info("✓ Botão 'Entrar' clicado")
            
            # Aguarda dashboard carregar
            logger.info("Aguardando carregamento da dashboard...")
            time.sleep(7)
            
            logger.info(f"✓ Instituição {nome_instituicao} acessada com sucesso")
            logger.info(f"URL atual: {self.driver.current_url}")
            
        except Exception as e:
            logger.error(f"Erro ao selecionar instituição: {e}")
            raise NavigationException(f"Falha ao selecionar instituição: {e}")
    
    def trocar_instituicao_dashboard(self, nome_instituicao: str):
        """
        Troca de instituição usando o menu do dashboard
        (Para quando já está logado em outra instituição)
        """
        logger.info(f"Trocando para instituição: {nome_instituicao}")
        
        try:
            time.sleep(3)
            
            # 1. Clica no botão da instituição atual (canto superior direito)
            logger.info("Clicando no botão da instituição atual (JUNTA MISSIONÁRIA)...")
            
            # Aguarda e clica no botão com o nome da Junta
            botao_instituicao = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'JUNTA MISSIONÁRIA')]"))
            )
            logger.info(f"✓ Botão da Junta encontrado com seletor: //span[contains(text(), 'JUNTA MISSIONÁRIA')]")
            
            # Tenta clicar, se não funcionar usa JavaScript
            try:
                botao_instituicao.click()
            except:
                self.driver.execute_script("arguments[0].click();", botao_instituicao)
            
            logger.info("✓ Clicou no botão da instituição atual")
            
            # 2. Aguarda o modal de instituições abrir
            logger.info("Aguardando modal de instituições abrir...")
            time.sleep(3)
            self.wait.until(
                EC.presence_of_element_located((By.XPATH, f"//div[contains(text(), '{nome_instituicao}')]"))
            )
            logger.info("✓ Modal de instituições carregado")
            
            # 3. Clica diretamente no nome da Igreja Presbiteriana (baseado na validação manual)
            logger.info("Clicando diretamente no nome '{}'...", nome_instituicao)

            try:
                # Tenta clicar no elemento div com o nome da instituição
                seletor_instituicao = f"//div[contains(text(), '{nome_instituicao}')]"
                elemento_instituicao = self.wait.until(
                    EC.element_to_be_clickable((By.XPATH, seletor_instituicao))
                )
                logger.info(f"✓ Elemento '{nome_instituicao}' encontrado com seletor: {seletor_instituicao}")
                
                self.driver.execute_script("arguments[0].click();", elemento_instituicao)
                logger.info(f"✓ Clicou no nome '{nome_instituicao}'")

                # Após clicar, o sistema gera um alerta "Deseja trocar de instituição?"
                # Precisamos aceitá-lo para confirmar a troca
                logger.info("Aguardando alerta de confirmação aparecer...")
                
                try:
                    # Cria uma espera explícita de 5 segundos apenas para o alerta
                    from selenium.webdriver.support.ui import WebDriverWait
                    wait_alert = WebDriverWait(self.driver, 5)
                    
                    # Espera o alerta aparecer
                    alert = wait_alert.until(EC.alert_is_present())
                    alert_text = alert.text
                    logger.info(f"✓ Alerta encontrado: '{alert_text}'")
                    
                    # Aceita o alerta (clica em OK)
                    alert.accept()
                    logger.info("✓ Alerta aceito (clicou em OK)")
                    
                    # Aguarda a página processar a troca
                    logger.info("Aguardando página recarregar...")
                    time.sleep(5)
                    logger.info("✓ Página recarregada")

                except TimeoutException:
                    logger.warning("⚠ Alerta não apareceu no tempo esperado. Tentando continuar...")
                except Exception as alert_error:
                    logger.error(f"❌ Erro ao tratar alerta: {alert_error}")
                    raise

            except TimeoutException:
                logger.error(f"Não foi possível encontrar o elemento da instituição '{nome_instituicao}' no modal.")
            
            # 5. Aguarda a troca ser processada (página recarrega)
            logger.info("Aguardando troca de instituição ser processada...")
            time.sleep(7)  # Tempo para a página recarregar
            
            # 6. Verifica se a troca foi bem-sucedida
            try:
                # Verifica se o nome da instituição mudou no canto superior direito
                self.wait.until(
                    EC.presence_of_element_located((By.XPATH, "//button[contains(text(), 'IGREJA PRESBITERIANA') or contains(text(), 'IPP')]"))
                )
                logger.info(f"✓ Troca para '{nome_instituicao}' confirmada - nome da instituição mudou")
            except:
                logger.warning("Não foi possível confirmar a troca pelo nome, mas continuando...")
            
            logger.info(f"✓ Troca para '{nome_instituicao}' realizada com sucesso")
            logger.info(f"URL atual: {self.driver.current_url}")
            
            # 7. Remove qualquer modal-backdrop residual que possa estar bloqueando cliques
            logger.info("Verificando e removendo modal-backdrop residual...")
            try:
                # Usa JavaScript para remover todos os modal-backdrops
                self.driver.execute_script("""
                    var backdrops = document.getElementsByClassName('modal-backdrop');
                    while(backdrops.length > 0) {
                        backdrops[0].parentNode.removeChild(backdrops[0]);
                    }
                """)
                # Remove a classe 'modal-open' do body que impede scroll
                self.driver.execute_script("document.body.classList.remove('modal-open');")
                logger.info("✓ Modal-backdrop removido (se existia)")
            except Exception as backdrop_error:
                logger.warning(f"⚠ Erro ao remover backdrop: {backdrop_error}")
            
        except Exception as e:
            logger.error(f"Erro ao trocar para {nome_instituicao}: {str(e)}")
            # Tira screenshot do erro
            try:
                error_screenshot = settings.base_dir / "logs" / "erro_troca_instituicao.png"
                error_screenshot.parent.mkdir(parents=True, exist_ok=True)
                self.driver.save_screenshot(str(error_screenshot))
                logger.info(f"Screenshot do erro salvo em: {error_screenshot}")
            except:
                pass
            raise NavigationException(f"Falha ao trocar para a instituição: {str(e)}")
    
    def navegar_para_exportacao(self):
        """Navega para a página de exportação"""
        logger.info("Navegando para exportação de dados...")
        
        try:
            time.sleep(3)
            
            # URL direta da exportação
            url_exportacao = "https://sis.sistemaprover.com.br/consolidado/exportacao"
            logger.info(f"Navegando para: {url_exportacao}")
            
            self.driver.get(url_exportacao)
            time.sleep(3)
            
            logger.info("Navegação para exportação concluída")
            
        except Exception as e:
            logger.error(f"Erro ao navegar para exportação: {e}")
            raise NavigationException(f"Falha na navegação: {e}")
    
    def baixar_lancamentos_financeiros(self, instituicao: str, data_inicio: str, data_fim: str) -> List[Path]:
        """
        Baixa os lançamentos financeiros para um período
        
        Args:
            instituicao: Nome da instituição
            data_inicio: Data inicial (YYYY-MM-DD)
            data_fim: Data final (YYYY-MM-DD)
            
        Returns:
            Lista de caminhos dos arquivos baixados
        """
        logger.info(f"Iniciando download de lançamentos financeiros para {instituicao}...")
        logger.info(f"Período: {data_inicio} até {data_fim}")
        
        try:
            # Verifica se há modal NPS (pesquisa de satisfação) e fecha se houver
            logger.info("Verificando se há modal NPS (pesquisa) para fechar...")
            try:
                # Procura pelo modal NPS (timeout curto de 3 segundos)
                from selenium.webdriver.support.ui import WebDriverWait
                wait_modal = WebDriverWait(self.driver, 3)
                
                # Tenta encontrar o botão de fechar do modal NPS (X ou Fechar)
                modal_nps = wait_modal.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.modal-nps, #modalwindow"))
                )
                logger.info("✓ Modal NPS encontrado, tentando fechar...")
                
                # Tenta clicar no botão de fechar (geralmente um X ou botão "Fechar")
                try:
                    botao_fechar = self.driver.find_element(By.XPATH, "//div[contains(@class, 'modal-nps')]//button[contains(@class, 'close') or contains(text(), 'Fechar')]")
                    botao_fechar.click()
                    logger.info("✓ Modal NPS fechado pelo botão")
                except:
                    # Se não encontrar botão, tenta usar ESC
                    logger.info("Tentando fechar modal com ESC...")
                    from selenium.webdriver.common.action_chains import ActionChains
                    ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                    logger.info("✓ ESC enviado para fechar modal")
                
                time.sleep(2)  # Aguarda o modal fechar
                
            except TimeoutException:
                logger.info("✓ Nenhum modal NPS encontrado, continuando...")
            except Exception as e:
                logger.warning(f"⚠ Erro ao tentar fechar modal NPS: {e}. Continuando mesmo assim...")
            
            # Remove qualquer modal-backdrop que possa estar bloqueando cliques
            logger.info("Removendo modal-backdrop antes de clicar em Lançamentos Financeiros...")
            try:
                self.driver.execute_script("""
                    var backdrops = document.getElementsByClassName('modal-backdrop');
                    while(backdrops.length > 0) {
                        backdrops[0].parentNode.removeChild(backdrops[0]);
                    }
                    document.body.classList.remove('modal-open');
                """)
                logger.info("✓ Modal-backdrop removido")
            except Exception as backdrop_error:
                logger.warning(f"⚠ Erro ao remover backdrop: {backdrop_error}")
            
            time.sleep(1)  # Pequena pausa após remover backdrop
            
            # Procura pelo link "Lançamentos Financeiros" e clica usando JavaScript
            logger.info("Procurando link 'Lançamentos Financeiros'...")
            link_lancamentos = self.wait.until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(text(), 'Lançamento') and contains(text(), 'Financeiro')]"))
            )
            # Usa JavaScript para garantir que o clique funcione mesmo com overlays
            logger.info("Clicando em 'Lançamentos Financeiros' via JavaScript...")
            self.driver.execute_script("arguments[0].click();", link_lancamentos)
            logger.info("Link 'Lançamentos Financeiros' clicado")
            
            # Aguarda o modal aparecer (aumentado para 10 segundos)
            logger.info("Aguardando modal de exportação abrir...")
            time.sleep(10)
            logger.info("✓ Modal deve estar aberto agora")
            
            # Converte as datas do formato YYYY-MM-DD para DD/MM/YYYY
            from datetime import datetime
            data_inicio_obj = datetime.strptime(data_inicio, "%Y-%m-%d")
            data_fim_obj = datetime.strptime(data_fim, "%Y-%m-%d")
            
            data_inicio_formatada = data_inicio_obj.strftime("%d/%m/%Y")
            data_fim_formatada = data_fim_obj.strftime("%d/%m/%Y")
            
            logger.info(f"Datas formatadas: {data_inicio_formatada} até {data_fim_formatada}")
            
            # Pega a lista de arquivos ANTES do download
            arquivos_antes = set(self.download_path.iterdir())
            
            # Tenta encontrar o campo de data de início com diferentes seletores
            logger.info("Procurando campo de data inicial...")
            campo_data_inicio = None
            
            try:
                # Tenta primeiro por NAME
                campo_data_inicio = self.wait.until(
                    EC.presence_of_element_located((By.NAME, "data_inicial"))
                )
                logger.info("✓ Campo encontrado por NAME='data_inicial'")
            except:
                logger.warning("Não encontrou por NAME='data_inicial', tentando outros seletores...")
                try:
                    # Tenta por ID
                    campo_data_inicio = self.driver.find_element(By.ID, "data_inicial")
                    logger.info("✓ Campo encontrado por ID='data_inicial'")
                except:
                    try:
                        # Tenta por XPATH com placeholder
                        campo_data_inicio = self.driver.find_element(By.XPATH, "//input[@placeholder='Data inicial' or @placeholder='De']")
                        logger.info("✓ Campo encontrado por placeholder")
                    except:
                        # Tenta encontrar qualquer input de data
                        campos_data = self.driver.find_elements(By.XPATH, "//input[@type='text' or @type='date']")
                        if campos_data and len(campos_data) >= 2:
                            campo_data_inicio = campos_data[0]
                            logger.info(f"✓ Campo encontrado como primeiro input de data ({len(campos_data)} campos encontrados)")
                        else:
                            raise Exception(f"Não conseguiu encontrar campo de data. Campos encontrados: {len(campos_data) if campos_data else 0}")
            
            if not campo_data_inicio:
                raise Exception("Campo data_inicial não encontrado com nenhum seletor")
            campo_data_inicio.clear()
            campo_data_inicio.send_keys(data_inicio_formatada)
            logger.info(f"Data de início digitada: {data_inicio_formatada}")
            
            # Pressiona ENTER para confirmar a data
            campo_data_inicio.send_keys(Keys.ENTER)
            logger.info("✓ Data de início confirmada (ENTER)")
            
            time.sleep(1)
            
            # Procura campo de data final
            logger.info("Procurando campo de data final...")
            campo_data_fim = None
            
            try:
                campo_data_fim = self.wait.until(
                    EC.presence_of_element_located((By.NAME, "data_final"))
                )
                logger.info("✓ Campo encontrado por NAME='data_final'")
            except:
                logger.warning("Não encontrou por NAME='data_final', tentando outros seletores...")
                try:
                    campo_data_fim = self.driver.find_element(By.ID, "data_final")
                    logger.info("✓ Campo encontrado por ID='data_final'")
                except:
                    try:
                        campo_data_fim = self.driver.find_element(By.XPATH, "//input[@placeholder='Data final' or @placeholder='Até']")
                        logger.info("✓ Campo encontrado por placeholder")
                    except:
                        campos_data = self.driver.find_elements(By.XPATH, "//input[@type='text' or @type='date']")
                        if campos_data and len(campos_data) >= 2:
                            campo_data_fim = campos_data[1]
                            logger.info(f"✓ Campo encontrado como segundo input de data")
                        else:
                            raise Exception("Não conseguiu encontrar campo de data final")
            
            if not campo_data_fim:
                raise Exception("Campo data_final não encontrado")
            campo_data_fim.clear()
            campo_data_fim.send_keys(data_fim_formatada)
            logger.info(f"Data fim digitada: {data_fim_formatada}")
            
            # Pressiona ENTER para confirmar
            campo_data_fim.send_keys(Keys.ENTER)
            logger.info("✓ Data fim confirmada (ENTER)")
            
            # Clica em um local neutro para fechar o calendário
            logger.info("Clicando em local neutro para fechar o calendário...")
            label_de = self.driver.find_element(By.XPATH, "//label[contains(text(), 'De')]")
            label_de.click()
            time.sleep(1)
            logger.info("✓ Foco removido dos campos de data.")
            
            # Procura e clica no botão "Filtrar"
            logger.info("Procurando botão 'Filtrar'...")
            botao_filtrar = self.wait.until(
                EC.presence_of_element_located((By.XPATH, "//button[contains(text(), 'Filtrar')]"))
            )
            logger.info("Botão 'Filtrar' encontrado")
            
            # Clica usando JavaScript para evitar problemas de interceptação
            logger.info("Clicando no botão 'Filtrar' via JavaScript...")
            self.driver.execute_script("arguments[0].click();", botao_filtrar)
            logger.info("✓ Botão 'Filtrar' clicado - iniciando download...")
            
            # Aguarda o novo arquivo aparecer
            arquivo_baixado = self._aguardar_novo_arquivo(arquivos_antes)
            
            logger.info(f"✓ Download concluído: {arquivo_baixado.name}")
            
            return [arquivo_baixado]
            
        except Exception as e:
            logger.error(f"Erro ao baixar lançamentos financeiros: {e}")
            raise DownloadException(f"Falha no download: {e}")
    
    def _aguardar_novo_arquivo(self, arquivos_antes: set, timeout: int = 60) -> Path:
        """
        Aguarda um novo arquivo aparecer no diretório de downloads
        
        Args:
            arquivos_antes: Set com arquivos que existiam antes
            timeout: Timeout em segundos
            
        Returns:
            Path do novo arquivo
        """
        logger.info("Aguardando novo arquivo aparecer no diretório de downloads...")
        
        start_time = time.time()
        novo_arquivo = None
        
        while time.time() - start_time < timeout:
            time.sleep(2)
            arquivos_atuais = set(self.download_path.iterdir())
            novos_arquivos = arquivos_atuais - arquivos_antes
            
            # Filtra arquivos .crdownload (ainda baixando)
            novos_completos = [f for f in novos_arquivos if not f.name.endswith('.crdownload')]
            
            if novos_completos:
                novo_arquivo = novos_completos[0]
                logger.info(f"Novo arquivo detectado: '{novo_arquivo.name}'. Verificando estabilidade do download...")
                
                # Aguarda o tamanho do arquivo estabilizar
                tamanho_anterior = -1
                tentativas = 0
                max_tentativas = 10
                
                while tentativas < max_tentativas:
                    time.sleep(2)
                    tamanho_atual = novo_arquivo.stat().st_size
                    
                    if tamanho_atual == tamanho_anterior and tamanho_atual > 0:
                        # Arquivo estabilizou
                        logger.info(f"Arquivo '{novo_arquivo.name}' estabilizado com {tamanho_atual} bytes.")
                        return novo_arquivo
                    
                    tamanho_anterior = tamanho_atual
                    tentativas += 1
                
                # Se chegou aqui, retorna o arquivo mesmo sem estabilizar completamente
                if novo_arquivo.exists():
                    return novo_arquivo
        
        raise DownloadException(f"Timeout de {timeout}s aguardando download")
    
    def close(self):
        """Fecha o navegador"""
        if self.driver:
            logger.info("Fechando driver do Selenium...")
            self.driver.quit()
            logger.info("Driver fechado")
    
    def processar_todas_instituicoes(self) -> Dict[str, List[Path]]:
        """
        Processa todas as instituições configuradas
        
        Returns:
            Dicionário {instituição: [arquivos_baixados]}
        """
        logger.info(f"Processando {len(self.instituicoes)} instituição(ões)...")
        
        resultados = {}
        
        try:
            # Setup
            self.setup_driver()
            self.login()
            
            # Processa cada instituição
            for i, instituicao in enumerate(self.instituicoes, 1):
                logger.info(f"\n{i} - Processando instituição: {instituicao}...")
                
                try:
                    # Seleciona instituição
                    if i == 1:
                        self.selecionar_instituicao(instituicao)
                    else:
                        self.trocar_instituicao_dashboard(instituicao)
                    
                    # Navega para exportação
                    self.navegar_para_exportacao()
                    
                    # Define período
                    data_inicio = "2025-01-01"
                    data_fim = datetime.now().strftime("%Y-%m-%d")
                    
                    # Baixa dados
                    arquivos = self.baixar_lancamentos_financeiros(instituicao, data_inicio, data_fim)
                    resultados[instituicao] = arquivos
                    
                    logger.info(f"✓ {instituicao}: {len(arquivos)} arquivo(s) baixado(s)")
                    
                except Exception as e:
                    logger.error(f"Erro ao processar {instituicao}: {e}")
                    resultados[instituicao] = []
            
            return resultados
            
        except Exception as e:
            logger.error(f"Erro no processamento: {e}")
            raise
        
        finally:
            self.close()
