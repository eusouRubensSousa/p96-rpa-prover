"""
RPA Automatizado para Sistema PROVER
Extrai dados financeiros com login e navegação totalmente automatizados
"""

import time
import tempfile
import os
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
        
        # Em servidor Linux sem DISPLAY, forca headless mesmo que .env esteja false.
        headless_mode = settings.headless_mode
        if not headless_mode and os.name != "nt" and not os.environ.get("DISPLAY"):
            logger.warning(
                "HEADLESS_MODE=false, mas DISPLAY nao encontrado. "
                "Forcando headless para ambiente servidor."
            )
            headless_mode = True

        # Modo headless
        if headless_mode:
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")

        # ----------------------------------------------------------------
        # Anti-detecção de automação
        # Metáfora: o Chrome headless sai de fábrica com um crachá escrito
        # "ROBÔ" — User-Agent "HeadlessChrome", navigator.webdriver=true,
        # sem plugins, sem idioma. O servidor lê esse crachá e retorna 403.
        # Aqui trocamos o crachá por um de "usuário Windows comum" antes
        # de bater na porta.
        # ----------------------------------------------------------------

        # 1) User-Agent de browser Windows real (sem "HeadlessChrome")
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )

        # 2) Idioma pt-BR para parecer usuário brasileiro comum
        options.add_argument("--lang=pt-BR")
        options.add_argument("--accept-lang=pt-BR,pt;q=0.9,en;q=0.8")

        # Opções de estabilidade (sem sandbox = obrigatório em container root)
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        
        # Inicializa driver
        driver_path = ChromeDriverManager().install()

        # Workaround para versões recentes do webdriver-manager/chromedriver:
        # em alguns ambientes, o path retornado aponta para o arquivo
        # THIRD_PARTY_NOTICES.chromedriver em vez do binário executável.
        if Path(driver_path).name.startswith("THIRD_PARTY_NOTICES"):
            candidate = Path(driver_path).with_name("chromedriver")
            if candidate.exists():
                driver_path = str(candidate)

        # Garante permissão de execução no binário final
        try:
            os.chmod(driver_path, 0o755)
        except Exception:
            pass

        service = Service(driver_path)
        self.driver = webdriver.Chrome(service=service, options=options)

        # 3) Remove navigator.webdriver=true via CDP
        # Injetado em TODA página antes do JS dela rodar — como trocar
        # a plaquinha "automação" antes da câmera ligar.
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['pt-BR', 'pt', 'en-US', 'en']
                });
                window.chrome = { runtime: {} };
            """
        })

        self.wait = WebDriverWait(self.driver, 30)
        logger.info("Driver configurado com sucesso")
    
    def login(self):
        """Faz login no sistema PROVER"""
        logger.info("Iniciando processo de login...")
        
        try:
            # ----------------------------------------------------------------
            # ETAPA 1 — Navega para a URL base e aguarda o redirect para /login
            # ----------------------------------------------------------------
            logger.info(f"Navegando para {settings.prover_url}")
            self.driver.get(settings.prover_url)

            logger.info("Aguardando redirect para /login...")
            try:
                WebDriverWait(self.driver, 15).until(
                    lambda d: "/login" in d.current_url
                )
                logger.info(f"✓ Redirect confirmado. URL atual: {self.driver.current_url}")
            except TimeoutException:
                logger.warning(
                    f"Redirect para /login nao ocorreu em 15s. URL atual: {self.driver.current_url}. "
                    "Continuando mesmo assim..."
                )

            # ----------------------------------------------------------------
            # ETAPA 2 — Aguarda o JavaScript renderizar o formulário no DOM
            # ----------------------------------------------------------------
            logger.info("Aguardando document.readyState == complete...")
            try:
                WebDriverWait(self.driver, 20).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                logger.info("✓ Página totalmente carregada (readyState=complete)")
            except TimeoutException:
                logger.warning("readyState nao atingiu 'complete' em 20s. Continuando...")

            logger.info("Aguardando formulario de login renderizar (input visivel)...")
            try:
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input"))
                )
                logger.info("✓ Formulario renderizado — input encontrado no DOM")
            except TimeoutException:
                logger.warning("Nenhum input encontrado em 20s. O JS pode estar lento ou bloqueado.")

            # Margem extra para frameworks SPA (React/Vue/Angular)
            time.sleep(2)

            # ----------------------------------------------------------------
            # ETAPA 3 — Salva screenshot e HTML APÓS o JS terminar
            # ----------------------------------------------------------------
            screenshot_path = settings.base_dir / "logs" / "login_page.png"
            screenshot_path.parent.mkdir(parents=True, exist_ok=True)
            self.driver.save_screenshot(str(screenshot_path))
            logger.info(f"Screenshot salvo em: {screenshot_path}")

            html_path = settings.base_dir / "logs" / "login_page.html"
            with open(str(html_path), "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            logger.info(f"HTML salvo em: {html_path}")

            # ----------------------------------------------------------------
            # ETAPA 4 — Busca de campos com fallback em iframes
            # ----------------------------------------------------------------
            def buscar_campo(selectores, nome_campo: str, preferred_frame: int = None):
                """
                Tenta encontrar um campo no DOM principal e, se necessario, dentro de iframes.
                Retorna (elemento, indice_iframe_ou_none, seletor_usado_ou_none).
                """
                # 1) Tenta primeiro em um iframe especifico
                if preferred_frame is not None:
                    try:
                        self.driver.switch_to.default_content()
                        iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
                        if preferred_frame < len(iframes):
                            self.driver.switch_to.frame(iframes[preferred_frame])
                            for by, seletor in selectores:
                                try:
                                    elemento = WebDriverWait(self.driver, 3).until(
                                        EC.visibility_of_element_located((by, seletor))
                                    )
                                    return elemento, preferred_frame, (by, seletor)
                                except TimeoutException:
                                    continue
                    except Exception:
                        pass

                # 2) Tenta no DOM principal
                self.driver.switch_to.default_content()
                for by, seletor in selectores:
                    try:
                        elemento = WebDriverWait(self.driver, 4).until(
                            EC.visibility_of_element_located((by, seletor))
                        )
                        return elemento, None, (by, seletor)
                    except TimeoutException:
                        logger.debug(f"Nao encontrou {nome_campo} com {by}='{seletor}' no DOM principal")

                # 3) Tenta em todos os iframes
                self.driver.switch_to.default_content()
                iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
                for idx, frame in enumerate(iframes):
                    try:
                        self.driver.switch_to.default_content()
                        self.driver.switch_to.frame(frame)
                        for by, seletor in selectores:
                            try:
                                elemento = WebDriverWait(self.driver, 3).until(
                                    EC.visibility_of_element_located((by, seletor))
                                )
                                return elemento, idx, (by, seletor)
                            except TimeoutException:
                                continue
                    except Exception:
                        continue

                self.driver.switch_to.default_content()
                return None, None, None
            
            # Preenche usuário
            logger.info("Procurando campo de usuário...")
            seletores_usuario = [
                (By.NAME, "username"),
                (By.NAME, "user"),
                (By.NAME, "login"),
                (By.NAME, "email"),
                (By.ID, "username"),
                (By.ID, "user"),
                (By.ID, "login"),
                (By.CSS_SELECTOR, "input[type='email']"),
                (By.CSS_SELECTOR, "input[type='text']")
            ]
            campo_usuario, frame_usuario, seletor_usuario = buscar_campo(seletores_usuario, "usuario")
            if not campo_usuario:
                raise LoginException(f"Campo de usuario nao encontrado. HTML salvo em: {html_path}")

            if frame_usuario is not None:
                logger.info(f"Campo de usuario encontrado no iframe {frame_usuario} com {seletor_usuario}")
            else:
                logger.info(f"Campo de usuario encontrado no DOM principal com {seletor_usuario}")

            logger.info(f"Placeholder usuario: {campo_usuario.get_attribute('placeholder')}")
            campo_usuario.clear()
            campo_usuario.send_keys(settings.prover_username)
            logger.info("✓ Usuário preenchido")
            
            # Preenche senha
            logger.info("Procurando campo de senha...")
            seletores_senha = [
                (By.NAME, "password"),
                (By.NAME, "senha"),
                (By.ID, "password"),
                (By.ID, "senha"),
                (By.CSS_SELECTOR, "input[type='password']")
            ]
            campo_senha, frame_senha, seletor_senha = buscar_campo(
                seletores_senha,
                "senha",
                preferred_frame=frame_usuario
            )
            if not campo_senha:
                raise LoginException(f"Campo de senha nao encontrado. HTML salvo em: {html_path}")

            if frame_senha is not None:
                logger.info(f"Campo de senha encontrado no iframe {frame_senha} com {seletor_senha}")
            else:
                logger.info(f"Campo de senha encontrado no DOM principal com {seletor_senha}")

            logger.info(f"Placeholder senha: {campo_senha.get_attribute('placeholder')}")
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
            
            screenshot_path = settings.base_dir / "logs" / "selecao_instituicao.png"
            screenshot_path.parent.mkdir(parents=True, exist_ok=True)
            self.driver.save_screenshot(str(screenshot_path))
            logger.info(f"Screenshot da seleção salvo em: {screenshot_path}")
            
            try:
                label_xpath = f"//label[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{nome_instituicao.lower()}')]"
                label = self.driver.find_element(By.XPATH, label_xpath)
                logger.info(f"✓ Encontrou label da instituição: {label.text}")
                label.click()
                time.sleep(1)
                logger.info(f"✓ {nome_instituicao} selecionada")
                
            except Exception as e:
                logger.warning(f"Não encontrou pelo label, tentando radio button direto: {e}")
                radios = self.driver.find_elements(By.CSS_SELECTOR, "input[type='radio'][name='instituicao']")
                logger.info(f"Encontrados {len(radios)} radio buttons")
                for radio in radios:
                    try:
                        radio.click()
                        time.sleep(0.5)
                        logger.info(f"Clicou no radio button")
                        break
                    except:
                        continue
            
            logger.info("Procurando botão 'Entrar' da seleção de instituição...")
            botao_entrar = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.busca-login[type='submit']"))
            )
            logger.info(f"Botão encontrado com seletor: button.busca-login[type='submit']")
            time.sleep(1)
            botao_texto = botao_entrar.text
            logger.info(f"Texto do botão: {botao_texto}")
            botao_entrar.click()
            logger.info("✓ Botão 'Entrar' clicado")
            
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
            
            logger.info("Clicando no botão da instituição atual (JUNTA MISSIONÁRIA)...")
            botao_instituicao = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'JUNTA MISSIONÁRIA')]"))
            )
            logger.info(f"✓ Botão da Junta encontrado")
            
            try:
                botao_instituicao.click()
            except:
                self.driver.execute_script("arguments[0].click();", botao_instituicao)
            
            logger.info("✓ Clicou no botão da instituição atual")
            
            logger.info("Aguardando modal de instituições abrir...")
            time.sleep(3)
            self.wait.until(
                EC.presence_of_element_located((By.XPATH, f"//div[contains(text(), '{nome_instituicao}')]"))
            )
            logger.info("✓ Modal de instituições carregado")

            try:
                seletor_instituicao = f"//div[contains(text(), '{nome_instituicao}')]"
                elemento_instituicao = self.wait.until(
                    EC.element_to_be_clickable((By.XPATH, seletor_instituicao))
                )
                logger.info(f"✓ Elemento '{nome_instituicao}' encontrado")
                self.driver.execute_script("arguments[0].click();", elemento_instituicao)
                logger.info(f"✓ Clicou no nome '{nome_instituicao}'")

                logger.info("Aguardando alerta de confirmação aparecer...")
                try:
                    wait_alert = WebDriverWait(self.driver, 5)
                    alert = wait_alert.until(EC.alert_is_present())
                    alert_text = alert.text
                    logger.info(f"✓ Alerta encontrado: '{alert_text}'")
                    alert.accept()
                    logger.info("✓ Alerta aceito (clicou em OK)")
                    time.sleep(5)
                    logger.info("✓ Página recarregada")
                except TimeoutException:
                    logger.warning("⚠ Alerta não apareceu no tempo esperado. Tentando continuar...")
                except Exception as alert_error:
                    logger.error(f"❌ Erro ao tratar alerta: {alert_error}")
                    raise

            except TimeoutException:
                logger.error(f"Não foi possível encontrar o elemento da instituição '{nome_instituicao}' no modal.")
            
            logger.info("Aguardando troca de instituição ser processada...")
            time.sleep(7)
            
            try:
                self.wait.until(
                    EC.presence_of_element_located((By.XPATH, "//button[contains(text(), 'IGREJA PRESBITERIANA') or contains(text(), 'IPP')]"))
                )
                logger.info(f"✓ Troca para '{nome_instituicao}' confirmada")
            except:
                logger.warning("Não foi possível confirmar a troca pelo nome, mas continuando...")
            
            logger.info(f"✓ Troca para '{nome_instituicao}' realizada com sucesso")
            logger.info(f"URL atual: {self.driver.current_url}")
            
            logger.info("Verificando e removendo modal-backdrop residual...")
            try:
                self.driver.execute_script("""
                    var backdrops = document.getElementsByClassName('modal-backdrop');
                    while(backdrops.length > 0) {
                        backdrops[0].parentNode.removeChild(backdrops[0]);
                    }
                """)
                self.driver.execute_script("document.body.classList.remove('modal-open');")
                logger.info("✓ Modal-backdrop removido (se existia)")
            except Exception as backdrop_error:
                logger.warning(f"⚠ Erro ao remover backdrop: {backdrop_error}")
            
        except Exception as e:
            logger.error(f"Erro ao trocar para {nome_instituicao}: {str(e)}")
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
            url_exportacao = "https://sis.sistemaprover.com.br/consolidado/exportacao"
            logger.info(f"Navegando para: {url_exportacao}")
            self.driver.get(url_exportacao)
            time.sleep(3)
            logger.info("Navegação para exportação concluída")
        except Exception as e:
            logger.error(f"Erro ao navegar para exportação: {e}")
            raise NavigationException(f"Falha na navegação: {e}")
    
    def _fechar_modais_overlay(self):
        """
        Fecha modais de overlay que possam estar bloqueando cliques (ex.: aviso de feriado,
        horário de atendimento, modal com #imgModal). Evita 'element click intercepted'.
        """
        try:
            try:
                img_modal = self.driver.find_element(By.ID, "imgModal")
                modal_container = self.driver.execute_script(
                    "return arguments[0].closest('.modal, [role=\"dialog\"], .modal-dialog') || arguments[0].parentElement;",
                    img_modal
                )
                if modal_container:
                    botao = modal_container.find_element(By.XPATH, ".//button[contains(text(), 'Fechar')]")
                    self.driver.execute_script("arguments[0].click();", botao)
                    logger.info("✓ Modal overlay (imgModal) fechado pelo botão 'Fechar'")
                    time.sleep(1)
                    return
            except NoSuchElementException:
                pass
            except Exception as e:
                logger.debug(f"Tentativa de fechar modal imgModal: {e}")
            
            try:
                botoes_fechar = self.driver.find_elements(
                    By.XPATH,
                    "//div[contains(@class, 'modal')]//button[contains(text(), 'Fechar')]"
                )
                for btn in botoes_fechar:
                    if btn.is_displayed():
                        self.driver.execute_script("arguments[0].click();", btn)
                        logger.info("✓ Modal overlay fechado (botão Fechar)")
                        time.sleep(1)
                        return
            except Exception as e:
                logger.debug(f"Tentativa de fechar modal por botão Fechar: {e}")
            
            from selenium.webdriver.common.action_chains import ActionChains
            ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(0.5)
        except Exception as e:
            logger.debug(f"_fechar_modais_overlay: {e}")
    
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
            logger.info("Verificando se há modal NPS (pesquisa) para fechar...")
            try:
                wait_modal = WebDriverWait(self.driver, 3)
                modal_nps = wait_modal.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.modal-nps, #modalwindow"))
                )
                logger.info("✓ Modal NPS encontrado, tentando fechar...")
                try:
                    botao_fechar = self.driver.find_element(By.XPATH, "//div[contains(@class, 'modal-nps')]//button[contains(@class, 'close') or contains(text(), 'Fechar')]")
                    botao_fechar.click()
                    logger.info("✓ Modal NPS fechado pelo botão")
                except:
                    logger.info("Tentando fechar modal com ESC...")
                    from selenium.webdriver.common.action_chains import ActionChains
                    ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                    logger.info("✓ ESC enviado para fechar modal")
                time.sleep(2)
            except TimeoutException:
                logger.info("✓ Nenhum modal NPS encontrado, continuando...")
            except Exception as e:
                logger.warning(f"⚠ Erro ao tentar fechar modal NPS: {e}. Continuando mesmo assim...")
            
            self._fechar_modais_overlay()
            
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
            
            time.sleep(1)
            
            logger.info("Procurando link 'Lançamentos Financeiros'...")
            link_lancamentos = self.wait.until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(text(), 'Lançamento') and contains(text(), 'Financeiro')]"))
            )
            logger.info("Clicando em 'Lançamentos Financeiros' via JavaScript...")
            self.driver.execute_script("arguments[0].click();", link_lancamentos)
            logger.info("Link 'Lançamentos Financeiros' clicado")
            
            logger.info("Aguardando modal de exportação abrir...")
            time.sleep(10)
            logger.info("✓ Modal deve estar aberto agora")
            
            from datetime import datetime
            data_inicio_obj = datetime.strptime(data_inicio, "%Y-%m-%d")
            data_fim_obj = datetime.strptime(data_fim, "%Y-%m-%d")
            data_inicio_formatada = data_inicio_obj.strftime("%d/%m/%Y")
            data_fim_formatada = data_fim_obj.strftime("%d/%m/%Y")
            logger.info(f"Datas formatadas: {data_inicio_formatada} até {data_fim_formatada}")
            
            arquivos_antes = set(self.download_path.iterdir())
            
            logger.info("Procurando campo de data inicial...")
            campo_data_inicio = None
            try:
                campo_data_inicio = self.wait.until(
                    EC.presence_of_element_located((By.NAME, "data_inicial"))
                )
                logger.info("✓ Campo encontrado por NAME='data_inicial'")
            except:
                logger.warning("Não encontrou por NAME='data_inicial', tentando outros seletores...")
                try:
                    campo_data_inicio = self.driver.find_element(By.ID, "data_inicial")
                    logger.info("✓ Campo encontrado por ID='data_inicial'")
                except:
                    try:
                        campo_data_inicio = self.driver.find_element(By.XPATH, "//input[@placeholder='Data inicial' or @placeholder='De']")
                        logger.info("✓ Campo encontrado por placeholder")
                    except:
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
            campo_data_inicio.send_keys(Keys.ENTER)
            logger.info("✓ Data de início confirmada (ENTER)")
            
            time.sleep(1)
            
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
            campo_data_fim.send_keys(Keys.ENTER)
            logger.info("✓ Data fim confirmada (ENTER)")
            
            self._fechar_modais_overlay()
            logger.info("Clicando em local neutro para fechar o calendário...")
            label_de = self.driver.find_element(By.XPATH, "//label[contains(text(), 'De')]")
            self.driver.execute_script("arguments[0].click();", label_de)
            time.sleep(1)
            logger.info("✓ Foco removido dos campos de data.")
            
            logger.info("Procurando botão 'Filtrar'...")
            botao_filtrar = self.wait.until(
                EC.presence_of_element_located((By.XPATH, "//button[contains(text(), 'Filtrar')]"))
            )
            logger.info("Botão 'Filtrar' encontrado")
            logger.info("Clicando no botão 'Filtrar' via JavaScript...")
            self.driver.execute_script("arguments[0].click();", botao_filtrar)
            logger.info("✓ Botão 'Filtrar' clicado - iniciando download...")
            
            arquivo_baixado = self._aguardar_novo_arquivo(arquivos_antes)
            logger.info(f"✓ Download concluído: {arquivo_baixado.name}")
            return [arquivo_baixado]
            
        except Exception as e:
            logger.error(f"Erro ao baixar lançamentos financeiros: {e}")
            raise DownloadException(f"Falha no download: {e}")
    
    def _aguardar_novo_arquivo(self, arquivos_antes: set, timeout: int = 60) -> Path:
        """
        Aguarda um novo arquivo aparecer no diretório de downloads.
        Ignora .crdownload e .tmp (Chrome pode renomear .tmp para o nome final).
        """
        logger.info("Aguardando novo arquivo aparecer no diretório de downloads...")
        extensoes_finais = (".xlsx", ".xls", ".csv", ".zip")
        start_time = time.time()
        novo_arquivo = None
        
        while time.time() - start_time < timeout:
            time.sleep(2)
            arquivos_atuais = set(self.download_path.iterdir())
            novos_arquivos = arquivos_atuais - arquivos_antes
            novos_completos = [
                f for f in novos_arquivos
                if not f.name.endswith(".crdownload") and not f.name.endswith(".tmp")
            ]
            com_ext_final = [f for f in novos_completos if f.suffix.lower() in extensoes_finais]
            candidatos = com_ext_final if com_ext_final else novos_completos
            
            if candidatos:
                novo_arquivo = candidatos[0]
                logger.info(f"Novo arquivo detectado: '{novo_arquivo.name}'. Verificando estabilidade do download...")
                tamanho_anterior = -1
                tentativas = 0
                max_tentativas = 10
                
                while tentativas < max_tentativas:
                    time.sleep(2)
                    if not novo_arquivo.exists():
                        arquivos_agora = set(self.download_path.iterdir())
                        novos_agora = arquivos_agora - arquivos_antes
                        finais = [f for f in novos_agora if f.suffix.lower() in extensoes_finais and f.exists()]
                        if finais:
                            logger.info(f"Arquivo renomeado pelo navegador; usando '{finais[0].name}'.")
                            return finais[0]
                        break
                    try:
                        tamanho_atual = novo_arquivo.stat().st_size
                    except OSError:
                        break
                    if tamanho_atual == tamanho_anterior and tamanho_atual > 0:
                        logger.info(f"Arquivo '{novo_arquivo.name}' estabilizado com {tamanho_atual} bytes.")
                        return novo_arquivo
                    tamanho_anterior = tamanho_atual
                    tentativas += 1
                
                if novo_arquivo.exists():
                    return novo_arquivo
                arquivos_agora = set(self.download_path.iterdir())
                novos_agora = arquivos_agora - arquivos_antes
                finais = [f for f in novos_agora if f.suffix.lower() in extensoes_finais and f.exists()]
                if finais:
                    return finais[0]
        
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
            self.setup_driver()
            self.login()
            
            for i, instituicao in enumerate(self.instituicoes, 1):
                logger.info(f"\n{i} - Processando instituição: {instituicao}...")
                try:
                    if i == 1:
                        self.selecionar_instituicao(instituicao)
                    else:
                        self.trocar_instituicao_dashboard(instituicao)
                    
                    self.navegar_para_exportacao()
                    
                    data_inicio = "2025-01-01"
                    data_fim = datetime.now().strftime("%Y-%m-%d")
                    
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