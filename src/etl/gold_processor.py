"""
Processador da Camada Gold
Transforma dados da camada Silver em modelo dimensional (Star Schema)
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple
import numpy as np

from google.cloud import storage
from config.settings import settings
from src.utils.logger import get_logger
from src.utils.exceptions import StorageException

logger = get_logger()


class GoldProcessor:
    """Processa dados da camada Silver para Gold (modelo dimensional)"""
    
    def __init__(self):
        """Inicializa o processador Gold"""
        self.bucket_name = settings.gcs_bucket_name
        self.client = None
        self.bucket = None
        self.df_silver = None
        
    def _initialize_client(self):
        """Inicializa o cliente do GCS"""
        try:
            logger.info("Inicializando cliente do Google Cloud Storage...")
            
            credentials_path = settings.google_application_credentials
            if credentials_path and Path(credentials_path).exists():
                self.client = storage.Client.from_service_account_json(credentials_path)
            else:
                self.client = storage.Client()
            
            self.bucket = self.client.bucket(self.bucket_name)
            logger.info(f"Cliente inicializado - Bucket: {self.bucket_name}")
            
        except Exception as e:
            raise StorageException(f"Erro ao inicializar cliente GCS: {e}")
    
    def _baixar_arquivo_silver(self) -> pd.DataFrame:
        """Baixa o arquivo consolidado da camada Silver"""
        logger.info("Baixando dados da camada Silver...")
        
        silver_path = "silver/fluxo_caixa_rpa/fluxo_caixa_consolidado.parquet"
        
        try:
            blob = self.bucket.blob(silver_path)
            
            # Baixa o Parquet
            from io import BytesIO
            buffer = BytesIO()
            blob.download_to_file(buffer)
            buffer.seek(0)
            
            # Lê o Parquet
            df = pd.read_parquet(buffer)
            
            logger.info(f"✓ Dados Silver carregados: {len(df)} linhas")
            return df
            
        except Exception as e:
            raise StorageException(f"Erro ao baixar arquivo Silver: {e}")
    
    def create_dim_instituicao(self) -> pd.DataFrame:
        """Cria dimensão de Instituição"""
        logger.info("Criando dim_instituicao...")
        
        df = self.df_silver[['instituicao']].copy()
        df = df.drop_duplicates().reset_index(drop=True)
        df['sk_instituicao'] = df.index + 1
        
        # Reordena colunas
        df = df[['sk_instituicao', 'instituicao']]
        
        logger.info(f"✓ dim_instituicao: {len(df)} registros")
        return df
    
    def create_dim_tipo_lancamento(self) -> pd.DataFrame:
        """Cria dimensão de Tipo de Lançamento"""
        logger.info("Criando dim_tipo_lancamento...")
        
        df = self.df_silver[['Tipo']].copy()  # Corrigido: coluna é "Tipo", não "Tipo de Lançamento"
        df.columns = ['tipo_lancamento']
        df['tipo_lancamento'] = df['tipo_lancamento'].fillna('Não Informado')
        df = df.drop_duplicates().reset_index(drop=True)
        df['sk_tipo_lancamento'] = df.index + 1
        
        df = df[['sk_tipo_lancamento', 'tipo_lancamento']]
        
        logger.info(f"✓ dim_tipo_lancamento: {len(df)} registros")
        return df
    
    def create_dim_categoria(self) -> pd.DataFrame:
        """Cria dimensão de Categoria"""
        logger.info("Criando dim_categoria...")
        
        # Usar os nomes EXATOS das colunas do CSV (15 e 17)
        df = self.df_silver[['Classfica\u00e7\u00e3o Categoria', 'Classfica\u00e7\u00e3o Item']].copy()
        df.columns = ['categoria', 'item']
        df['categoria'] = df['categoria'].fillna('Não Alocado')
        df['item'] = df['item'].fillna('Não Alocado')
        df = df.drop_duplicates().reset_index(drop=True)
        df['sk_categoria'] = df.index + 1
        
        df = df[['sk_categoria', 'categoria', 'item']]
        
        logger.info(f"✓ dim_categoria: {len(df)} registros")
        return df
    
    def create_dim_conta(self) -> pd.DataFrame:
        """Cria dimensão de Conta"""
        logger.info("Criando dim_conta...")
        
        df = self.df_silver[['Conta']].copy()
        df.columns = ['conta']
        df['conta'] = df['conta'].fillna('Não Informado')
        df = df.drop_duplicates().reset_index(drop=True)
        df['sk_conta'] = df.index + 1
        
        df = df[['sk_conta', 'conta']]
        
        logger.info(f"✓ dim_conta: {len(df)} registros")
        return df
    
    def create_dim_centro_custo(self) -> pd.DataFrame:
        """Cria dimensão de Centro de Custo"""
        logger.info("Criando dim_centro_custo...")
        
        df = self.df_silver[['Centro Custo']].copy()
        df.columns = ['centro_custo']
        df['centro_custo'] = df['centro_custo'].fillna('Não Alocado')
        df = df.drop_duplicates().reset_index(drop=True)
        df['sk_centro_custo'] = df.index + 1
        
        df = df[['sk_centro_custo', 'centro_custo']]
        
        logger.info(f"✓ dim_centro_custo: {len(df)} registros")
        return df
    
    def create_dim_forma_pagamento(self) -> pd.DataFrame:
        """Cria dimensão de Forma de Pagamento"""
        logger.info("Criando dim_forma_pagamento...")
        
        df = self.df_silver[['Forma Pagamento']].copy()
        df.columns = ['forma_pagamento']
        df['forma_pagamento'] = df['forma_pagamento'].fillna('Não Informado')
        df = df.drop_duplicates().reset_index(drop=True)
        df['sk_forma_pagamento'] = df.index + 1
        
        df = df[['sk_forma_pagamento', 'forma_pagamento']]
        
        logger.info(f"✓ dim_forma_pagamento: {len(df)} registros")
        return df
    
    def create_dim_fornecedor(self) -> pd.DataFrame:
        """Cria dimensão de Fornecedor"""
        logger.info("Criando dim_fornecedor...")
        
        df = self.df_silver[['Fornecedor']].copy()
        df.columns = ['fornecedor']
        df['fornecedor'] = df['fornecedor'].fillna('Não Informado')
        df = df.drop_duplicates().reset_index(drop=True)
        df['sk_fornecedor'] = df.index + 1
        
        df = df[['sk_fornecedor', 'fornecedor']]
        
        logger.info(f"✓ dim_fornecedor: {len(df)} registros")
        return df
    
    def create_dim_tempo(self) -> pd.DataFrame:
        """Cria dimensão de Tempo"""
        logger.info("Criando dim_tempo...")
        
        # Converte coluna de data
        df = self.df_silver[['Data']].copy()
        df['data'] = pd.to_datetime(df['Data'], errors='coerce')
        
        # Remove datas inválidas
        df = df[df['data'].notna()].copy()
        
        # Extrai atributos temporais
        df['ano'] = df['data'].dt.year
        df['mes'] = df['data'].dt.month
        df['dia'] = df['data'].dt.day
        df['trimestre'] = df['data'].dt.quarter
        df['dia_semana'] = df['data'].dt.dayofweek + 1
        df['nome_mes'] = df['data'].dt.month_name()
        df['nome_dia_semana'] = df['data'].dt.day_name()
        
        # Remove duplicatas por data
        df = df[['data', 'ano', 'mes', 'dia', 'trimestre', 'dia_semana', 'nome_mes', 'nome_dia_semana']]
        df = df.drop_duplicates().reset_index(drop=True)
        df['sk_tempo'] = df.index + 1
        
        # Reordena
        df = df[['sk_tempo', 'data', 'ano', 'mes', 'dia', 'trimestre', 'dia_semana', 'nome_mes', 'nome_dia_semana']]
        
        logger.info(f"✓ dim_tempo: {len(df)} registros")
        return df
    
    def create_fato_fluxo_caixa(self, dimensoes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Cria tabela fato de Fluxo de Caixa"""
        logger.info("Criando fato_fluxo_caixa...")
        
        # Copia dados originais
        fato = self.df_silver.copy()
        
        # Converte data
        fato['data'] = pd.to_datetime(fato['Data'], errors='coerce')
        
        # Faz merge com dimensões para obter as chaves substitutas (SKs)
        
        # dim_instituicao
        fato = fato.merge(
            dimensoes['dim_instituicao'][['sk_instituicao', 'instituicao']],
            on='instituicao',
            how='left'
        )
        
        # dim_tipo_lancamento
        fato['tipo_lancamento_temp'] = fato['Tipo'].fillna('Não Informado')
        fato = fato.merge(
            dimensoes['dim_tipo_lancamento'].rename(columns={'tipo_lancamento': 'tipo_lancamento_temp'}),
            on='tipo_lancamento_temp',
            how='left'
        )
        
        # dim_categoria
        fato['categoria_temp'] = fato['Classfica\u00e7\u00e3o Categoria'].fillna('Não Alocado')
        fato['item_temp'] = fato['Classfica\u00e7\u00e3o Item'].fillna('Não Alocado')
        fato = fato.merge(
            dimensoes['dim_categoria'].rename(columns={'categoria': 'categoria_temp', 'item': 'item_temp'}),
            on=['categoria_temp', 'item_temp'],
            how='left'
        )
        
        # dim_conta
        fato['conta_temp'] = fato['Conta'].fillna('Não Informado')
        fato = fato.merge(
            dimensoes['dim_conta'].rename(columns={'conta': 'conta_temp'}),
            on='conta_temp',
            how='left'
        )
        
        # dim_centro_custo
        fato['centro_custo_temp'] = fato['Centro Custo'].fillna('Não Alocado')
        fato = fato.merge(
            dimensoes['dim_centro_custo'].rename(columns={'centro_custo': 'centro_custo_temp'}),
            on='centro_custo_temp',
            how='left'
        )
        
        # dim_forma_pagamento
        fato['forma_pagamento_temp'] = fato['Forma Pagamento'].fillna('Não Informado')
        fato = fato.merge(
            dimensoes['dim_forma_pagamento'].rename(columns={'forma_pagamento': 'forma_pagamento_temp'}),
            on='forma_pagamento_temp',
            how='left'
        )
        
        # dim_fornecedor
        fato['fornecedor_temp'] = fato['Fornecedor'].fillna('Não Informado')
        fato = fato.merge(
            dimensoes['dim_fornecedor'].rename(columns={'fornecedor': 'fornecedor_temp'}),
            on='fornecedor_temp',
            how='left'
        )
        
        # dim_tempo
        fato = fato.merge(
            dimensoes['dim_tempo'][['sk_tempo', 'data']],
            on='data',
            how='left'
        )
        
        # Seleciona e renomeia colunas finais - removido sk_status pois não existe coluna Status
        fato_final = pd.DataFrame({
            'data': fato['data'],
            'sk_instituicao': fato['sk_instituicao'],
            'sk_tipo_lancamento': fato['sk_tipo_lancamento'],
            'sk_categoria': fato['sk_categoria'],
            'sk_conta': fato['sk_conta'],
            'sk_centro_custo': fato['sk_centro_custo'],
            'sk_forma_pagamento': fato['sk_forma_pagamento'],
            'sk_fornecedor': fato['sk_fornecedor'],
            'sk_tempo': fato['sk_tempo'],
            'valor': fato['Valor'],
            'historico': fato['Hist\u00f3rico'],
            'numero_documento': fato['N\u00famero Documento'],
            'data_hora_coleta': datetime.now()
        })
        
        logger.info(f"✓ fato_fluxo_caixa: {len(fato_final)} registros")
        return fato_final
    
    def _salvar_parquet(self, df: pd.DataFrame, nome_tabela: str) -> str:
        """Salva DataFrame como Parquet no GCS"""
        
        gold_path = f"gold/gold_rpa/{nome_tabela}.parquet"
        
        # Converte para Parquet em memória
        from io import BytesIO
        buffer = BytesIO()
        df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
        buffer.seek(0)
        
        # Upload
        blob = self.bucket.blob(gold_path)
        blob.metadata = {
            'data_processamento': datetime.now().isoformat(),
            'num_linhas': str(len(df)),
            'num_colunas': str(len(df.columns))
        }
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        
        gcs_full_path = f"gs://{self.bucket_name}/{gold_path}"
        logger.info(f"  ✓ {nome_tabela}: {len(df)} linhas -> {gcs_full_path}")
        
        return gcs_full_path
    
    def _salvar_metadata(self, dimensoes: Dict[str, pd.DataFrame], fato: pd.DataFrame):
        """Salva metadados do processamento"""
        
        metadata = {
            'data_processamento': datetime.now().isoformat(),
            'bucket': self.bucket_name,
            'caminho_base': 'gold/gold_rpa/',
            'dimensoes': {
                nome: {
                    'num_linhas': len(df),
                    'colunas': list(df.columns)
                }
                for nome, df in dimensoes.items()
            },
            'fato': {
                'nome': 'fato_fluxo_caixa',
                'num_linhas': len(fato),
                'colunas': list(fato.columns)
            }
        }
        
        # Salva JSON no GCS
        metadata_path = "gold/gold_rpa/metadata.json"
        blob = self.bucket.blob(metadata_path)
        blob.upload_from_string(
            json.dumps(metadata, indent=2),
            content_type='application/json'
        )
        
        logger.info(f"✓ Metadata salvo: gs://{self.bucket_name}/{metadata_path}")
    
    def _validar_qualidade_dados(self, dimensoes: Dict[str, pd.DataFrame], fato: pd.DataFrame):
        """Valida a qualidade dos dados processados"""
        logger.info("\nValidando qualidade dos dados...")
        
        # Verifica se há registros
        if len(fato) == 0:
            raise ValueError("Tabela fato está vazia!")
        
        # Verifica chaves nulas na fato
        sk_columns = [col for col in fato.columns if col.startswith('sk_')]
        for col in sk_columns:
            nulls = fato[col].isna().sum()
            if nulls > 0:
                logger.warning(f"⚠ {col} tem {nulls} valores nulos ({nulls/len(fato)*100:.1f}%)")
        
        logger.info("✓ Validação de qualidade concluída")
    
    def processar(self) -> Dict[str, str]:
        """
        Executa o processamento completo da camada Gold
        
        Returns:
            Dicionário com caminhos dos arquivos gerados
        """
        logger.info("="*80)
        logger.info("INICIANDO PROCESSAMENTO DA CAMADA GOLD - MODELO DIMENSIONAL")
        logger.info("="*80)
        
        try:
            # Inicializa cliente
            self._initialize_client()
            
            # Baixa dados Silver
            self.df_silver = self._baixar_arquivo_silver()
            
            # Cria dimensões (removido dim_status pois não existe coluna Status no Silver)
            logger.info("\nCriando dimensões...")
            dimensoes = {
                'dim_instituicao': self.create_dim_instituicao(),
                'dim_tipo_lancamento': self.create_dim_tipo_lancamento(),
                'dim_categoria': self.create_dim_categoria(),
                'dim_conta': self.create_dim_conta(),
                'dim_centro_custo': self.create_dim_centro_custo(),
                'dim_forma_pagamento': self.create_dim_forma_pagamento(),
                'dim_fornecedor': self.create_dim_fornecedor(),
                'dim_tempo': self.create_dim_tempo()
            }
            
            # Cria tabela fato
            logger.info("\nCriando tabela fato...")
            fato = self.create_fato_fluxo_caixa(dimensoes)
            
            # Valida qualidade
            self._validar_qualidade_dados(dimensoes, fato)
            
            # Salva no GCS
            logger.info("\nSalvando dados no GCS...")
            caminhos = {}
            
            for nome, df in dimensoes.items():
                caminhos[nome] = self._salvar_parquet(df, nome)
            
            caminhos['fato_fluxo_caixa'] = self._salvar_parquet(fato, 'fato_fluxo_caixa')
            
            # Salva metadata
            self._salvar_metadata(dimensoes, fato)
            
            logger.info("\n" + "="*80)
            logger.info("✅ PROCESSAMENTO GOLD CONCLUÍDO COM SUCESSO!")
            logger.info("="*80)
            logger.info(f"Total de tabelas geradas: {len(caminhos)}")
            logger.info(f"  - Dimensões: {len(dimensoes)}")
            logger.info(f"  - Fato: 1")
            
            return caminhos
            
        except Exception as e:
            logger.error(f"Erro no processamento Gold: {e}")
            raise StorageException(f"Falha no processamento Gold: {e}")

