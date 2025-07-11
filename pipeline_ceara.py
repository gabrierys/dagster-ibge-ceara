# pipeline_ceara.py
from dagster import asset, Definitions
import pandas as pd
from sidrapy import get_sidra_table

# Código da tabela de estimativas populacionais
SIDRA_TABLE = 6579

@asset
def fetch_ceara_population() -> pd.DataFrame:
    # territorial_level=6 -> municípios
    df = get_sidra_table(
        SIDRA_TABLE,
        territorial_level=6,
        region='all',  # todos os municípios do Brasil
        classific=['c1'],  # total
        period='last'  # último ano disponível
    )
    # Filtra apenas municípios do código 23 (Ceará)
    df_ce = df[df['Region'].str.startswith('23')]  # código IBGE do Ceará :contentReference[oaicite:4]{index=4}
    # Seleciona colunas úteis e renomeia
    df_ce = df_ce[['Region', 'Region Name', 'Value']]
    df_ce.columns = ['codigo_municipio', 'municipio', 'populacao']
    return df_ce

@asset
def save_ceara_csv(fetch_ceara_population: pd.DataFrame):
    fetch_ceara_population.to_csv('populacao_municipios_ceara.csv', index=False)

defs = Definitions(assets=[fetch_ceara_population, save_ceara_csv])
