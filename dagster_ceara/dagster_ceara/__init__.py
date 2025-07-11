import pandas as pd
import requests
from dagster import asset, Definitions, AssetExecutionContext, MetadataValue
from datetime import datetime
import logging


from dagster import ScheduleDefinition, DefaultScheduleStatus, SkipReason, schedule
from dagster import SensorDefinition, SensorEvaluationContext, RunRequest, sensor, DefaultSensorStatus
import os
from datetime import timedelta


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SIDRA_URL = "https://apisidra.ibge.gov.br/values/t/6579/n6/in%20n3%2023/v/allxp/p/last/f/n"

@asset(
    description="Busca dados populacionais dos municípios do Ceará na API SIDRA",
    metadata={
        "fonte": "IBGE - API SIDRA",
        "tabela": "6579 - Estimativas da População",
        "estado": "Ceará",
        "codigo_ibge": "23"
    }
)
def fetch_ceara_population(context: AssetExecutionContext) -> pd.DataFrame:
    logger.info("Iniciando busca de dados populacionais do Ceará")
    response = requests.get(SIDRA_URL, params={"formato": "json"}, timeout=30)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data)
    logger.info(f"Dados recebidos da API: {len(df)} registros")
    if len(df) > 1:
        df = df.iloc[1:].reset_index(drop=True)
    if 'D2N' in df.columns:
        df = df[df['D2N'] == 'População residente estimada'].copy()
    if 'D1N' in df.columns and 'V' in df.columns:
        df_clean = df[['D1N', 'V', 'D3N']].copy()
        df_clean.columns = ['municipio', 'populacao', 'ano']
        df_clean['populacao'] = pd.to_numeric(df_clean['populacao'], errors='coerce')
        df_clean = df_clean.dropna()
        df_clean['data_extracao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        total_municipios = len(df_clean)
        populacao_total = df_clean['populacao'].sum()
        maior_municipio = df_clean.loc[df_clean['populacao'].idxmax(), 'municipio']
        menor_municipio = df_clean.loc[df_clean['populacao'].idxmin(), 'municipio']
        context.add_output_metadata({
            "total_municipios": total_municipios,
            "populacao_total": f"{populacao_total:,.0f}",
            "populacao_media": f"{df_clean['populacao'].mean():,.0f}",
            "maior_municipio": maior_municipio,
            "menor_municipio": menor_municipio,
            "ano_referencia": df_clean['ano'].iloc[0] if not df_clean.empty else "N/A",
            "preview": MetadataValue.md(df_clean.head().to_markdown())
        })
        logger.info(f"Dados processados: {total_municipios} municípios")
        logger.info(f"População total: {populacao_total:,.0f}")
        return df_clean
    else:
        logger.warning("Estrutura de dados inesperada - retornando dados brutos")
        return df

@asset(
    description="Salva os dados populacionais em formato CSV",
    metadata={
        "formato": "CSV",
        "codificacao": "UTF-8",
        "separador": ","
    }
)
def save_ceara_csv(context: AssetExecutionContext, fetch_ceara_population: pd.DataFrame):
    """Salva os dados populacionais em formato CSV"""
    
    logger.info("Salvando dados em CSV")
    
    # Salva o arquivo CSV
    filename = "populacao_municipios_ceara.csv"
    fetch_ceara_population.to_csv(filename, index=False, encoding='utf-8')
    
    # Metadados
    context.add_output_metadata({
        "arquivo": filename,
        "registros_salvos": len(fetch_ceara_population),
        "colunas": list(fetch_ceara_population.columns),
        "tamanho_arquivo": f"{len(fetch_ceara_population) * len(fetch_ceara_population.columns)} cells"
    })
    
    logger.info(f"Arquivo salvo: {filename} com {len(fetch_ceara_population)} registros")


@asset(
    description="Calcula estatísticas detalhadas dos dados populacionais",
    metadata={
        "tipo": "Analytics",
        "output": "Estatísticas descritivas"
    }
)
def population_statistics(context: AssetExecutionContext, fetch_ceara_population: pd.DataFrame) -> dict:
    """Calcula estatísticas descritivas dos dados populacionais"""
    
    logger.info("Calculando estatísticas descritivas")
    
    # Calcula estatísticas
    stats = {
        "total_municipios": len(fetch_ceara_population),
        "populacao_total": int(fetch_ceara_population['populacao'].sum()),
        "populacao_media": float(fetch_ceara_population['populacao'].mean()),
        "populacao_mediana": float(fetch_ceara_population['populacao'].median()),
        "populacao_max": int(fetch_ceara_population['populacao'].max()),
        "populacao_min": int(fetch_ceara_population['populacao'].min()),
        "desvio_padrao": float(fetch_ceara_population['populacao'].std()),
        "municipio_mais_populoso": fetch_ceara_population.loc[
            fetch_ceara_population['populacao'].idxmax(), 'municipio'
        ],
        "municipio_menos_populoso": fetch_ceara_population.loc[
            fetch_ceara_population['populacao'].idxmin(), 'municipio'
        ],
        "data_processamento": datetime.now().isoformat()
    }
    
    # Top 10 municípios mais populosos
    top_10 = fetch_ceara_population.nlargest(10, 'populacao')[['municipio', 'populacao']]
    
    # Bottom 10 municípios menos populosos  
    bottom_10 = fetch_ceara_population.nsmallest(10, 'populacao')[['municipio', 'populacao']]
    
    # Adiciona metadados
    context.add_output_metadata({
        "populacao_total": f"{stats['populacao_total']:,}",
        "populacao_media": f"{stats['populacao_media']:,.0f}",
        "maior_municipio": stats['municipio_mais_populoso'],
        "menor_municipio": stats['municipio_menos_populoso'],
        "top_10_municipios": MetadataValue.md(top_10.to_markdown()),
        "bottom_10_municipios": MetadataValue.md(bottom_10.to_markdown()),
        "distribuicao": MetadataValue.md(fetch_ceara_population['populacao'].describe().to_markdown())
    })
    
    logger.info(f"Estatísticas calculadas para {stats['total_municipios']} municípios")
    logger.info(f"População total: {stats['populacao_total']:,}")
    
    return stats


@asset(
    description="Valida a qualidade dos dados populacionais",
    metadata={
        "tipo": "Data Quality",
        "checks": "Completude, consistência, outliers"
    }
)
def data_quality_check(context: AssetExecutionContext, fetch_ceara_population: pd.DataFrame) -> dict:
    """Valida a qualidade dos dados populacionais"""
    
    logger.info("Executando verificações de qualidade dos dados")
    
    # Verificações de qualidade
    checks = {
        "total_registros": len(fetch_ceara_population),
        "registros_completos": len(fetch_ceara_population.dropna()),
        "valores_nulos": {
            col: int(fetch_ceara_population[col].isnull().sum()) 
            for col in fetch_ceara_population.columns
        },
        "valores_duplicados": int(fetch_ceara_population['municipio'].duplicated().sum()),
        "municipios_unicos": len(fetch_ceara_population['municipio'].unique()),
        "populacao_zero_negativa": int((fetch_ceara_population['populacao'] <= 0).sum()),
        "outliers_populacao": []
    }
    
    # Detecta outliers usando IQR
    Q1 = fetch_ceara_population['populacao'].quantile(0.25)
    Q3 = fetch_ceara_population['populacao'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    outliers = fetch_ceara_population[
        (fetch_ceara_population['populacao'] < lower_bound) | 
        (fetch_ceara_population['populacao'] > upper_bound)
    ]
    
    if not outliers.empty:
        checks["outliers_populacao"] = outliers[['municipio', 'populacao']].to_dict('records')
    
    # Score de qualidade (0-100)
    completude = (checks["registros_completos"] / checks["total_registros"]) * 100
    unicidade = ((checks["total_registros"] - checks["valores_duplicados"]) / checks["total_registros"]) * 100
    consistencia = ((checks["total_registros"] - checks["populacao_zero_negativa"]) / checks["total_registros"]) * 100
    
    quality_score = (completude + unicidade + consistencia) / 3
    checks["quality_score"] = round(quality_score, 2)
    
    # Status da qualidade
    if quality_score >= 95:
        status = "EXCELENTE"
    elif quality_score >= 90:
        status = "BOA"
    elif quality_score >= 80:
        status = "REGULAR"
    else:
        status = "RUIM"
    
    checks["status_qualidade"] = status
    
    # Metadados
    context.add_output_metadata({
        "quality_score": f"{quality_score:.1f}%",
        "status": status,
        "completude": f"{completude:.1f}%",
        "registros_validos": f"{checks['registros_completos']}/{checks['total_registros']}",
        "outliers_detectados": len(outliers),
        "verificacoes": MetadataValue.md(f"""
## Verificações de Qualidade

- **Completude**: {completude:.1f}%
- **Unicidade**: {unicidade:.1f}%
- **Consistência**: {consistencia:.1f}%
- **Score Geral**: {quality_score:.1f}%
- **Status**: {status}

### Detalhes
- Registros totais: {checks['total_registros']}
- Registros completos: {checks['registros_completos']}
- Valores duplicados: {checks['valores_duplicados']}
- População zero/negativa: {checks['populacao_zero_negativa']}
- Outliers detectados: {len(outliers)}
        """)
    })
    
    logger.info(f"Qualidade dos dados: {status} ({quality_score:.1f}%)")
    
    return checks

# Schedule para execução diária
@schedule(
    job_name="__ASSET_JOB",
    cron_schedule="0 6 * * *",  # Todo dia às 6:00 AM
    default_status=DefaultScheduleStatus.STOPPED  # Inicia pausado
)
def daily_population_update():
    """Executa o pipeline diariamente às 6:00 AM"""
    return {}

# Schedule para execução semanal
@schedule(
    job_name="__ASSET_JOB", 
    cron_schedule="0 6 * * 1",  # Segunda-feira às 6:00 AM
    default_status=DefaultScheduleStatus.STOPPED
)
def weekly_population_update():
    """Executa o pipeline semanalmente às segundas-feiras"""
    return {}

@sensor(
    job_name="__ASSET_JOB",
    default_status=DefaultSensorStatus.STOPPED
)
def csv_file_sensor(context: SensorEvaluationContext):
    csv_path = "populacao_municipios_ceara.csv"
    if not os.path.exists(csv_path):
        return SkipReason("Arquivo CSV não existe ainda")
    file_modified = os.path.getmtime(csv_path)
    current_time = datetime.now().timestamp()
    if current_time - file_modified > 24 * 3600:
        return RunRequest(
            run_key=f"csv_update_{int(current_time)}",
            tags={"triggered_by": "csv_file_sensor"}
        )
    return SkipReason("Arquivo CSV foi modificado recentemente")

defs = Definitions(
    assets=[
        fetch_ceara_population,
        save_ceara_csv,
        population_statistics,
        data_quality_check
    ],
    schedules=[
        daily_population_update,
        weekly_population_update
    ],
    sensors=[
        csv_file_sensor
    ]
)
