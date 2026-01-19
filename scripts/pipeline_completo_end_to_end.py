"""
Pipeline Completo End-to-End - RPA IPP

Este script executa todo o pipeline de dados das instituições IPP:
1. Extração RPA da Junta Missionária de Pinheiros
2. Extração RPA da Igreja Presbiteriana de Pinheiros  
3. Processamento da camada Silver (consolidação)
4. Processamento da camada Gold (modelo dimensional)
5. Carregamento no BigQuery (Data Warehouse)

Uso:
    python scripts/pipeline_completo_end_to_end.py
    
Opções:
    --skip-bronze    Pula a extração RPA (usa dados existentes)
    --only-silver    Executa apenas Silver + Gold + BigQuery
    --only-gold      Executa apenas Gold + BigQuery
    --only-bq        Executa apenas carregamento BigQuery
"""

import sys
import argparse
from pathlib import Path
import traceback
from datetime import datetime
import subprocess
import time

# Adiciona o diretório raiz ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import get_logger
from src.utils.exceptions import StorageException

logger = get_logger()


def executar_comando_python(script_path: str, descricao: str) -> bool:
    """
    Executa um script Python e retorna se foi bem-sucedido
    
    Args:
        script_path: Caminho para o script Python
        descricao: Descrição da etapa para logs
        
    Returns:
        True se sucesso, False se erro
    """
    try:
        logger.info(f"Executando: {descricao}")
        logger.info(f"Script: {script_path}")
        
        # Executa o script
        resultado = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent
        )
        
        if resultado.returncode == 0:
            logger.info(f"✅ {descricao} - SUCESSO")
            # Log apenas as últimas linhas para não poluir
            if resultado.stdout:
                linhas = resultado.stdout.strip().split('\n')
                for linha in linhas[-5:]:  # Últimas 5 linhas
                    if linha.strip():
                        logger.info(f"  {linha}")
            return True
        else:
            logger.error(f"❌ {descricao} - ERRO")
            logger.error(f"Código de saída: {resultado.returncode}")
            if resultado.stderr:
                logger.error(f"Erro: {resultado.stderr}")
            if resultado.stdout:
                logger.error(f"Saída: {resultado.stdout}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Erro ao executar {descricao}: {str(e)}")
        return False


def executar_extracao_bronze(skip_bronze: bool = False) -> bool:
    """
    Executa a extração da camada Bronze (RPA)
    
    Args:
        skip_bronze: Se True, pula a extração
        
    Returns:
        True se sucesso, False se erro
    """
    if skip_bronze:
        logger.info("⏭️ Pulando extração Bronze (--skip-bronze)")
        return True
    
    logger.info("\n" + "="*80)
    logger.info("ETAPA 1: EXTRAÇÃO DA CAMADA BRONZE - RPA")
    logger.info("="*80)
    
    # 1.1 Extração Junta Missionária
    sucesso_junta = executar_comando_python(
        "scripts/extract_and_upload_prover.py",
        "Extração RPA - Junta Missionária de Pinheiros"
    )
    
    if not sucesso_junta:
        logger.error("❌ Falha na extração da Junta Missionária")
        return False
    
    # Aguarda um pouco entre as extrações para não sobrecarregar o sistema
    logger.info("⏳ Aguardando 30 segundos entre extrações...")
    time.sleep(30)
    
    # 1.2 Extração Igreja Presbiteriana
    sucesso_ipp = executar_comando_python(
        "scripts/extract_and_upload_prover_ipp.py",
        "Extração RPA - Igreja Presbiteriana de Pinheiros"
    )
    
    if not sucesso_ipp:
        logger.error("❌ Falha na extração da Igreja Presbiteriana")
        return False
    
    logger.info("✅ Extração Bronze concluída com sucesso!")
    return True


def executar_processamento_silver() -> bool:
    """
    Executa o processamento da camada Silver
    
    Returns:
        True se sucesso, False se erro
    """
    logger.info("\n" + "="*80)
    logger.info("ETAPA 2: PROCESSAMENTO DA CAMADA SILVER")
    logger.info("="*80)
    
    sucesso = executar_comando_python(
        "scripts/process_silver_layer.py",
        "Processamento Silver - Consolidação de dados"
    )
    
    if sucesso:
        logger.info("✅ Processamento Silver concluído com sucesso!")
    else:
        logger.error("❌ Falha no processamento Silver")
    
    return sucesso


def executar_processamento_gold() -> bool:
    """
    Executa o processamento da camada Gold
    
    Returns:
        True se sucesso, False se erro
    """
    logger.info("\n" + "="*80)
    logger.info("ETAPA 3: PROCESSAMENTO DA CAMADA GOLD - MODELO DIMENSIONAL")
    logger.info("="*80)
    
    sucesso = executar_comando_python(
        "scripts/process_gold_layer.py",
        "Processamento Gold - Modelo dimensional (Star Schema)"
    )
    
    if sucesso:
        logger.info("✅ Processamento Gold concluído com sucesso!")
    else:
        logger.error("❌ Falha no processamento Gold")
    
    return sucesso


def executar_carregamento_bigquery() -> bool:
    """
    Executa o carregamento no BigQuery
    
    Returns:
        True se sucesso, False se erro
    """
    logger.info("\n" + "="*80)
    logger.info("ETAPA 4: CARREGAMENTO NO BIGQUERY - DATA WAREHOUSE")
    logger.info("="*80)
    
    sucesso = executar_comando_python(
        "scripts/load_gold_to_bigquery.py",
        "Carregamento BigQuery - Data Warehouse (lille-422512.P96_IPP)"
    )
    
    if sucesso:
        logger.info("✅ Carregamento BigQuery concluído com sucesso!")
    else:
        logger.error("❌ Falha no carregamento BigQuery")
    
    return sucesso


def exibir_resumo_final(inicio_execucao: datetime, etapas_executadas: list) -> None:
    """
    Exibe resumo final da execução
    
    Args:
        inicio_execucao: Timestamp do início
        etapas_executadas: Lista de etapas executadas
    """
    fim_execucao = datetime.now()
    duracao_total = (fim_execucao - inicio_execucao).total_seconds()
    
    logger.info("\n" + "="*80)
    logger.info("🎯 PIPELINE COMPLETO FINALIZADO!")
    logger.info("="*80)
    
    logger.info(f"⏱️  Duração total: {duracao_total/60:.1f} minutos ({duracao_total:.0f} segundos)")
    logger.info(f"🕐 Início: {inicio_execucao.strftime('%H:%M:%S')}")
    logger.info(f"🕐 Fim: {fim_execucao.strftime('%H:%M:%S')}")
    
    logger.info(f"\n📋 Etapas executadas:")
    for i, etapa in enumerate(etapas_executadas, 1):
        logger.info(f"  {i}. ✅ {etapa}")
    
    logger.info(f"\n📊 Estrutura Final Completa:")
    logger.info("🗄️ Google Cloud Storage (p96_ipp/):")
    logger.info("├── bronze/")
    logger.info("│   ├── junta_missionaria/ (dados brutos RPA)")
    logger.info("│   └── ipp/ (dados brutos RPA)")
    logger.info("├── silver/")
    logger.info("│   └── fluxo_caixa_rpa/")
    logger.info("│       └── fluxo_caixa_consolidado.parquet")
    logger.info("└── gold/")
    logger.info("    └── gold_rpa/")
    logger.info("        ├── fato_fluxo_caixa.parquet")
    logger.info("        ├── dim_*.parquet (9 dimensões)")
    logger.info("        └── metadata.json")
    
    logger.info(f"\n🏢 BigQuery Data Warehouse (lille-422512.P96_IPP):")
    logger.info("├── fato_fluxo_caixa_rpa (tabela fato)")
    logger.info("└── dim_*_rpa (9 dimensões)")
    
    logger.info(f"\n🚀 Próximos passos sugeridos:")
    logger.info("  • Conectar Power BI/Looker Studio ao BigQuery")
    logger.info("  • Criar dashboards de análise financeira")
    logger.info("  • Agendar execução automática deste pipeline")
    logger.info("  • Implementar alertas para anomalias nos dados")
    
    logger.info("\n" + "="*80)
    logger.info("🎊 PIPELINE DE DADOS IPP CONCLUÍDO COM SUCESSO!")
    logger.info("="*80)


def main():
    """Função principal do pipeline completo"""
    
    # Configuração de argumentos
    parser = argparse.ArgumentParser(
        description="Pipeline Completo End-to-End - RPA IPP",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python scripts/pipeline_completo_end_to_end.py                    # Pipeline completo
  python scripts/pipeline_completo_end_to_end.py --skip-bronze     # Pula extração RPA
  python scripts/pipeline_completo_end_to_end.py --only-silver     # Apenas Silver + Gold + BQ
  python scripts/pipeline_completo_end_to_end.py --only-gold       # Apenas Gold + BQ
  python scripts/pipeline_completo_end_to_end.py --only-bq         # Apenas BigQuery
        """
    )
    
    parser.add_argument(
        "--skip-bronze", 
        action="store_true",
        help="Pula a extração RPA (usa dados Bronze existentes)"
    )
    parser.add_argument(
        "--only-silver",
        action="store_true", 
        help="Executa apenas Silver + Gold + BigQuery"
    )
    parser.add_argument(
        "--only-gold",
        action="store_true",
        help="Executa apenas Gold + BigQuery"
    )
    parser.add_argument(
        "--only-bq",
        action="store_true",
        help="Executa apenas carregamento BigQuery"
    )
    
    args = parser.parse_args()
    
    # Validação de argumentos mutuamente exclusivos
    opcoes_exclusivas = [args.only_silver, args.only_gold, args.only_bq]
    if sum(opcoes_exclusivas) > 1:
        logger.error("❌ Erro: Use apenas uma das opções --only-*")
        sys.exit(1)
    
    # Início da execução
    inicio_execucao = datetime.now()
    etapas_executadas = []
    
    logger.info("="*80)
    logger.info("🚀 INICIANDO PIPELINE COMPLETO END-TO-END - RPA IPP")
    logger.info("="*80)
    logger.info(f"Data/Hora: {inicio_execucao.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("Objetivo: Extração RPA → Silver → Gold → BigQuery")
    logger.info("Instituições: Junta Missionária + Igreja Presbiteriana")
    logger.info("Destino: lille-422512.P96_IPP (BigQuery)")
    logger.info("="*80)
    
    try:
        # ETAPA 1: Extração Bronze (RPA)
        if not args.only_silver and not args.only_gold and not args.only_bq:
            sucesso_bronze = executar_extracao_bronze(args.skip_bronze)
            if not sucesso_bronze:
                logger.error("❌ Pipeline interrompido: Falha na extração Bronze")
                sys.exit(1)
            if not args.skip_bronze:
                etapas_executadas.append("Extração RPA (Bronze)")
        
        # ETAPA 2: Processamento Silver
        if not args.only_gold and not args.only_bq:
            sucesso_silver = executar_processamento_silver()
            if not sucesso_silver:
                logger.error("❌ Pipeline interrompido: Falha no processamento Silver")
                sys.exit(1)
            etapas_executadas.append("Processamento Silver (Consolidação)")
        
        # ETAPA 3: Processamento Gold
        if not args.only_bq:
            sucesso_gold = executar_processamento_gold()
            if not sucesso_gold:
                logger.error("❌ Pipeline interrompido: Falha no processamento Gold")
                sys.exit(1)
            etapas_executadas.append("Processamento Gold (Modelo Dimensional)")
        
        # ETAPA 4: Carregamento BigQuery
        sucesso_bq = executar_carregamento_bigquery()
        if not sucesso_bq:
            logger.error("❌ Pipeline interrompido: Falha no carregamento BigQuery")
            sys.exit(1)
        etapas_executadas.append("Carregamento BigQuery (Data Warehouse)")
        
        # Resumo final
        exibir_resumo_final(inicio_execucao, etapas_executadas)
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Pipeline interrompido pelo usuário (Ctrl+C)")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"\n❌ ERRO INESPERADO NO PIPELINE: {e}")
        logger.error(f"Detalhes do erro:\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()





