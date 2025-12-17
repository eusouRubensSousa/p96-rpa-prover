"""
Exceções personalizadas para o projeto RPA PROVER
"""


class ProverRPAException(Exception):
    """Exceção base para todas as exceções do RPA PROVER"""
    pass


class LoginException(ProverRPAException):
    """Exceção para erros de autenticação no sistema PROVER"""
    pass


class NavigationException(ProverRPAException):
    """Exceção para erros de navegação no sistema PROVER"""
    pass


class DownloadException(ProverRPAException):
    """Exceção para erros durante o download de arquivos"""
    pass


class UploadException(ProverRPAException):
    """Exceção para erros durante o upload de arquivos"""
    pass


class StorageException(ProverRPAException):
    """Exceção para erros de armazenamento no GCS"""
    pass


class ETLException(ProverRPAException):
    """Exceção para erros durante o processamento ETL"""
    pass


class BigQueryException(ProverRPAException):
    """Exceção para erros de carga no BigQuery"""
    pass






