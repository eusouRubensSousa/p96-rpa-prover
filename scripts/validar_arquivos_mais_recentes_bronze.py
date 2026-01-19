#!/usr/bin/env python3
"""
Script para validar quais arquivos Bronze mais recentes serão processados
Confirma que os arquivos mais atuais estão sendo selecionados para as tabelas RPA
"""
import sys
from pathlib import Path

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google.cloud import storage
from config.settings import settings
from src.utils.logger import setup_logger, get_logger

# Configura logger
setup_logger()
logger = get_logger()


def validar_arquivos_mais_recentes():
    """
    Valida quais arquivos Bronze mais recentes serão processados
    """
    print("="*80)
    print("VALIDAÇÃO: ARQUIVOS MAIS RECENTES DA CAMADA BRONZE")
    print("="*80)
    print()
    print("📋 Este script valida quais arquivos serão processados para as tabelas RPA")
    print("   (deve corresponder aos arquivos mais recentes de cada pasta Bronze)")
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
        
        if not bucket.exists():
            print(f"❌ ERRO: Bucket '{bucket_name}' não existe!")
            return
        
        print("✓ Cliente inicializado")
        print()
        
        arquivos_selecionados = {}
        
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
                
                # Filtra apenas arquivos CSV (mesma lógica do silver_processor.py)
                csv_blobs = [b for b in blobs if b.name.endswith('.csv')]
                
                if not csv_blobs:
                    print(f"   ⚠ Nenhum arquivo CSV encontrado")
                    print()
                    continue
                
                # Recarrega blobs para obter metadados completos
                for blob in csv_blobs:
                    blob.reload()
                
                # Ordena por data de criação (mais recente primeiro) - mesma lógica do silver_processor.py
                csv_blobs.sort(
                    key=lambda x: x.time_created if x.time_created else x.updated,
                    reverse=True
                )
                
                # Seleciona o mais recente (mesma lógica do silver_processor.py)
                arquivo_mais_recente = csv_blobs[0]
                
                arquivos_selecionados[nome_instituicao] = {
                    'nome': arquivo_mais_recente.name,
                    'data_criacao': arquivo_mais_recente.time_created or arquivo_mais_recente.updated,
                    'tamanho_mb': (arquivo_mais_recente.size / (1024 * 1024)) if arquivo_mais_recente.size else 0,
                    'total_arquivos': len(csv_blobs)
                }
                
                # Exibe resultado
                nome_arquivo = Path(arquivo_mais_recente.name).name
                print(f"   ✅ ARQUIVO SELECIONADO (mais recente):")
                print(f"      📄 Nome: {nome_arquivo}")
                print(f"      📅 Data de criação: {arquivo_mais_recente.time_created or arquivo_mais_recente.updated}")
                print(f"      📊 Tamanho: {(arquivo_mais_recente.size / (1024 * 1024)):.2f} MB")
                print()
                
                # Mostra os próximos arquivos para comparação
                if len(csv_blobs) > 1:
                    print(f"   📋 Comparação com outros arquivos ({len(csv_blobs)} total):")
                    for i, blob in enumerate(csv_blobs[:3], 1):  # Mostra os 3 mais recentes
                        nome = Path(blob.name).name
                        data = blob.time_created or blob.updated
                        tamanho = (blob.size / (1024 * 1024)) if blob.size else 0
                        marcador = "👉" if i == 1 else "  "
                        print(f"      {marcador} {i}. {nome}")
                        print(f"         📅 {data}")
                        print(f"         📊 {tamanho:.2f} MB")
                    if len(csv_blobs) > 3:
                        print(f"      ... e mais {len(csv_blobs) - 3} arquivo(s) mais antigo(s)")
                print()
                
            except Exception as e:
                print(f"   ❌ ERRO ao processar pasta: {e}")
                print()
        
        # Resumo final
        print("="*80)
        print("📊 RESUMO: ARQUIVOS QUE SERÃO PROCESSADOS PARA AS TABELAS RPA")
        print("="*80)
        print()
        
        if not arquivos_selecionados:
            print("⚠ Nenhum arquivo foi selecionado!")
        else:
            for instituicao, info in arquivos_selecionados.items():
                nome_arquivo = Path(info['nome']).name
                print(f"✅ {instituicao}:")
                print(f"   📄 Arquivo: {nome_arquivo}")
                print(f"   📅 Data: {info['data_criacao']}")
                print(f"   📊 Tamanho: {info['tamanho_mb']:.2f} MB")
                print(f"   📁 Total de arquivos na pasta: {info['total_arquivos']}")
                print()
            
            print("="*80)
            print("✅ VALIDAÇÃO CONCLUÍDA")
            print("="*80)
            print()
            print("💡 Estes são os arquivos que serão processados pelo SilverProcessor")
            print("   e que aparecerão nas tabelas RPA do BigQuery.")
            print()
        
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    """
    Executa a validação dos arquivos mais recentes
    """
    try:
        validar_arquivos_mais_recentes()
    except KeyboardInterrupt:
        print("\n\n⚠ Processo interrompido pelo usuário (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
