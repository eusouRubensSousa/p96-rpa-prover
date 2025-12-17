"""
Sistema de Logging usando Loguru
Configuração centralizada de logs para o projeto
"""
import sys
from pathlib import Path
from loguru import logger
from config.settings import settings


def setup_logger() -> None:
    """
    Configura o sistema de logging do projeto
    - Remove handlers padrão
    - Adiciona handler para console (colorido)
    - Adiciona handler para arquivo (com rotação)
    """
    # Remove handlers padrão do loguru
    logger.remove()
    
    # Cria diretório de logs se não existir
    settings.logs_dir.mkdir(parents=True, exist_ok=True)
    
    # Handler para console (colorido e formatado)
    logger.add(
        sys.stderr,
        format=settings.log_format,
        level=settings.log_level,
        colorize=True,
        backtrace=True,
        diagnose=True,
    )
    
    # Handler para arquivo (com rotação)
    log_file = settings.logs_dir / "rpa_prover.log"
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        level=settings.log_level,
        rotation=settings.log_rotation,
        retention=settings.log_retention,
        compression=settings.log_compression,
        backtrace=True,
        diagnose=True,
        encoding="utf-8",
    )
    
    logger.info(f"Logger configurado - Nível: {settings.log_level}")
    logger.info(f"Logs salvos em: {log_file}")


def get_logger():
    """
    Retorna a instância do logger configurado
    
    Returns:
        Logger configurado
    """
    return logger


# Aliases para facilitar o uso
def debug(message: str, **kwargs):
    """Log de debug"""
    logger.debug(message, **kwargs)


def info(message: str, **kwargs):
    """Log de informação"""
    logger.info(message, **kwargs)


def warning(message: str, **kwargs):
    """Log de aviso"""
    logger.warning(message, **kwargs)


def error(message: str, **kwargs):
    """Log de erro"""
    logger.error(message, **kwargs)


def critical(message: str, **kwargs):
    """Log crítico"""
    logger.critical(message, **kwargs)


def exception(message: str, **kwargs):
    """Log de exceção (inclui traceback)"""
    logger.exception(message, **kwargs)


# Exemplo de uso
if __name__ == "__main__":
    setup_logger()
    
    logger.debug("Este é um log de DEBUG")
    logger.info("Este é um log de INFO")
    logger.warning("Este é um log de WARNING")
    logger.error("Este é um log de ERROR")
    
    try:
        1 / 0
    except Exception:
        logger.exception("Este é um log de EXCEPTION com traceback")
