#!/usr/bin/env python3
"""
Script para listar arquivos das pastas Bronze no GCS
Apenas imprime os resultados, não salva em lugar algum
"""
import sys
from pathlib import Path
from datetime import datetime

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google.cloud import storage
from config.settings import settings
from src.utils.logger import setup_logger, get_logger

# Configura logger
setup_logger()
logger = get_logger()


def listar_arquivos_bronze():
    """
    Lista e imprime todos os arquivos das pastas Bronze
    """
    print("="*80)
    print("LISTAGEM DE ARQUIVOS DA CAMADA BRONZE")
    print("="*80)
    print()
    
    # Configurações
    bucket_name = settings.gcs_bucket_name
    project_id = settings.gcp_project_id or "lille-422512"
    
    # Instituições a processar (mesmas do silver_processor.py)
    instituicoes = {
        "junta_missionaria": "JUNTA MISSIONÁRIA DE PINHEIROS",
        "ipp": "IGREJA PRESBITERIANA DE PINHEIROS"
    }
    
    try:
        # Inicializa cliente GCS
        print(f"Inicializando cliente GCS...")
        print(f"  Bucket: {bucket_name}")
        print(f"  Projeto: {project_id}")
        print()
        
        credentials_path = settings.google_application_credentials
        if credentials_path and Path(credentials_path).exists():
            client = storage.Client.from_service_account_json(
                credentials_path,
                project=project_id
            )
        else:
            client = storage.Client(project=project_id)
        
        bucket = client.bucket(bucket_name)
        
        # Verifica se bucket existe
        if not bucket.exists():
            print(f"❌ ERRO: Bucket '{bucket_name}' não existe!")
            return
        
        print("✓ Cliente inicializado com sucesso")
        print()
        
        total_arquivos = 0
        
        # Para cada pasta Bronze
        for pasta, nome_instituicao in instituicoes.items():
            prefix = f"bronze/{pasta}/"
            
            print("-"*80)
            print(f"📁 PASTA: {prefix}")
            print(f"   Instituição: {nome_instituicao}")
            print("-"*80)
            
            try:
                # Lista todos os blobs com o prefixo
                blobs = list(bucket.list_blobs(prefix=prefix))
                
                if not blobs:
                    print(f"   ⚠ Nenhum arquivo encontrado nesta pasta")
                    print()
                    continue
                
                # Recarrega blobs para obter metadados completos
                arquivos_info = []
                for blob in blobs:
                    try:
                        blob.reload()
                        arquivos_info.append({
                            'nome': blob.name,
                            'tamanho_bytes': blob.size or 0,
                            'tamanho_mb': (blob.size / (1024 * 1024)) if blob.size else 0,
                            'time_created': blob.time_created,
                            'updated': blob.updated,
                            'content_type': blob.content_type or 'N/A'
                        })
                    except Exception as e:
                        print(f"   ⚠ Erro ao recarregar {blob.name}: {e}")
                        arquivos_info.append({
                            'nome': blob.name,
                            'tamanho_bytes': blob.size or 0,
                            'tamanho_mb': (blob.size / (1024 * 1024)) if blob.size else 0,
                            'time_created': None,
                            'updated': blob.updated,
                            'content_type': blob.content_type or 'N/A'
                        })
                
                # Ordena por data de criação (mais recente primeiro)
                arquivos_info.sort(
                    key=lambda x: x['time_created'] if x['time_created'] else x['updated'],
                    reverse=True
                )
                
                print(f"   📊 Total de arquivos encontrados: {len(arquivos_info)}")
                print()
                
                # Imprime informações de cada arquivo
                for i, arquivo in enumerate(arquivos_info, 1):
                    nome_arquivo = Path(arquivo['nome']).name
                    data_criacao = arquivo['time_created'] or arquivo['updated']
                    fonte_data = "criação" if arquivo['time_created'] else "modificação"
                    
                    print(f"   {i}. {nome_arquivo}")
                    print(f"      📅 Data de {fonte_data}: {data_criacao}")
                    print(f"      📊 Tamanho: {arquivo['tamanho_mb']:.2f} MB ({arquivo['tamanho_bytes']:,} bytes)")
                    print(f"      📄 Tipo: {arquivo['content_type']}")
                    print()
                
                total_arquivos += len(arquivos_info)
                
            except Exception as e:
                print(f"   ❌ ERRO ao listar arquivos: {e}")
                print()
        
        print("="*80)
        print(f"✅ RESUMO: Total de {total_arquivos} arquivo(s) encontrado(s) em todas as pastas Bronze")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    """
    Executa a listagem de arquivos Bronze
    """
    try:
        listar_arquivos_bronze()
    except KeyboardInterrupt:
        print("\n\n⚠ Processo interrompido pelo usuário (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
