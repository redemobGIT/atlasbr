"""
AtlasBR - Core Catalog for Census Data.

This module defines the "Contract" for every Census theme:
1. What table/URL to fetch from.
2. Which raw columns are required.
3. What strategy to use (BigQuery vs FTP).
"""

from typing import Literal, List, Dict, Optional, Tuple, TypeAlias
from pydantic import BaseModel, Field, ConfigDict

# --- Type Definitions ---

CensusStrategy: TypeAlias = Literal["bd_table", "ftp_csv"]

# --- Constants ---

# Base dos Dados Table IDs
BD_TABLE_BASIC_2010 = "basedosdados.br_ibge_censo_demografico.setor_censitario_basico_2010"
BD_TABLE_AGE_2010 = "basedosdados.br_ibge_censo_demografico.setor_censitario_idade_total_2010"
BD_TABLE_RACE_2010 = "basedosdados.br_ibge_censo_demografico.setor_censitario_raca_idade_genero_2010"
BD_TABLE_SETOR_2022 = "basedosdados.br_ibge_censo_2022.setor_censitario"

# IBGE FTP URLs
URL_BASIC_2022 = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/"
    "Agregados_por_setores_basico_BR_20250417.zip"
)
URL_INCOME_2022 = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Agregados_por_Setores_Censitarios_Rendimento_do_Responsavel/"
    "Agregados_por_setores_renda_responsavel_BR_csv.zip"
)
URL_RACE_2022 = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/"
    "Agregados_por_setores_cor_ou_raca_BR.zip"
)


def _generate_column_names(prefix: str, start: int, end: int, width: int = 3) -> List[str]:
    """Generates a sequence of zero-padded column names (e.g., 'v001', 'v002')."""
    return [f"{prefix}{i:0{width}d}" for i in range(start, end)]


# --- Domain Models ---

class CensusThemeSpec(BaseModel):
    """
    Defines the configuration contract for fetching a specific Census theme.
    Instances are immutable (frozen).
    """
    model_config = ConfigDict(frozen=True)

    theme: str
    year: int
    strategy: CensusStrategy

    # Strategy: bd_table
    table_id: Optional[str] = None
    required_columns: List[str] = Field(default_factory=list)

    # Strategy: ftp_csv
    url: Optional[str] = None
    csv_sep: str = ";"
    csv_encoding: str = "latin1"
    column_map: Dict[str, str] = Field(default_factory=dict)


# --- Registry Definition ---

CENSUS_CATALOG: List[CensusThemeSpec] = [
    # --- 2010 ---
    CensusThemeSpec(
        theme="basic", year=2010, strategy="bd_table",
        table_id=BD_TABLE_BASIC_2010, required_columns=["v001", "v002"]
    ),
    CensusThemeSpec(
        theme="income", year=2010, strategy="bd_table",
        table_id=BD_TABLE_BASIC_2010, required_columns=["v009"]
    ),
    CensusThemeSpec(
        theme="age", year=2010, strategy="bd_table",
        table_id=BD_TABLE_AGE_2010,
        required_columns=["v022"] + _generate_column_names("v", 35, 135, width=3)
    ),
    CensusThemeSpec(
        theme="race", year=2010, strategy="bd_table",
        table_id=BD_TABLE_RACE_2010,
        required_columns=["v002", "v003", "v004", "v005", "v006"]
    ),

    # --- 2022 ---
    CensusThemeSpec(
        theme="basic", year=2022, strategy="bd_table",
        table_id=BD_TABLE_SETOR_2022,
        required_columns=["domicilios", "pessoas"]
    ),
    CensusThemeSpec(
        theme="basic", year=2022, strategy="ftp_csv",
        url=URL_BASIC_2022,
        column_map={
            "CD_SETOR": "id_setor_censitario",
            "v0001": "habitantes",
            "v0002": "domicilios"
        },
    ),
    CensusThemeSpec(
        theme="income", year=2022, strategy="ftp_csv",
        url=URL_INCOME_2022,
        column_map={
            "CD_SETOR": "id_setor_censitario",
            "V06004": "rendimento_medio"
        },
    ),
    CensusThemeSpec(
        theme="age", year=2022, strategy="bd_table",
        table_id=BD_TABLE_SETOR_2022,
        required_columns=(
            ["pessoas", "V00644"] + _generate_column_names("V", 645, 657, width=5)
        ),
    ),
    CensusThemeSpec(
        theme="race", year=2022, strategy="bd_table",
        table_id=BD_TABLE_SETOR_2022,
        required_columns=(
            ["pessoas"] +
            _generate_column_names("V", 644, 657, width=5) +
            _generate_column_names("V", 657, 717, width=5)
        ),
    ),
    CensusThemeSpec(
        theme="race", year=2022, strategy="ftp_csv",
        url=URL_RACE_2022,
        column_map={
            "CD_SETOR": "id_setor_censitario",
            "V01317": "cor_branca", "V01318": "cor_preta",
            "V01319": "cor_amarela", "V01320": "cor_parda",
            "V01321": "cor_indigena"
        }
    )
]

# --- Optimization: O(1) Lookup Index ---

_CATALOG_INDEX: Dict[Tuple[str, int, CensusStrategy], CensusThemeSpec] = {
    (spec.theme, spec.year, spec.strategy): spec
    for spec in CENSUS_CATALOG
}


def get_theme_spec(theme: str, year: int, strategy: CensusStrategy) -> CensusThemeSpec:
    """
    Retrieves the configuration for a given theme, year, and strategy.

    Raises:
        ValueError: If no matching configuration exists in the catalog.
    """
    key = (theme, year, strategy)
    if key not in _CATALOG_INDEX:
        raise ValueError(
            f"No configuration found in CENSUS_CATALOG for theme='{theme}', "
            f"year={year}, strategy='{strategy}'"
        )
    return _CATALOG_INDEX[key]