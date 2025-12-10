"""
AtlasBR - Core Catalog for Census Data.

This module defines the "Contract" for every Census theme:
1. What table/URL to fetch from.
2. Which raw columns are required.
3. What strategy to use (BigQuery vs FTP).
"""

from typing import Literal, List, Dict, Optional, Union
from pydantic import BaseModel, Field, ConfigDict

# --- Constants ---

# Base dos Dados Table IDs
BD_TABLE_BASIC_2010 = (
    "basedosdados.br_ibge_censo_demografico.setor_censitario_basico_2010"
)
BD_TABLE_AGE_2010 = (
    "basedosdados.br_ibge_censo_demografico.setor_censitario_idade_total_2010"
)
BD_TABLE_RACE_2010 = (
    "basedosdados.br_ibge_censo_demografico.setor_censitario_raca_idade_genero_2010"
)
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


# --- Helpers for Column Generation ---


def _gen_cols(prefix: str, start: int, end: int, width: int = 3) -> List[str]:
    """Generates a sequence of column names (e.g., v035, v036...)."""
    return [f"{prefix}{i:0{width}d}" for i in range(start, end)]


# --- Domain Models ---


class CensusThemeSpec(BaseModel):
    """Defines how to fetch a specific Census theme."""

    model_config = ConfigDict(frozen=True)

    theme: str
    year: int
    strategy: Literal["bd_table", "ftp_csv"]

    # Strategy: bd_table
    table_id: Optional[str] = None
    required_columns: List[str] = Field(default_factory=list)

    # Strategy: ftp_csv
    url: Optional[str] = None
    csv_sep: str = ";"
    csv_encoding: str = "latin1"
    csv_decimal: str = "."
    column_map: Dict[str, str] = Field(default_factory=dict)


# --- The Catalog Registry ---

CENSUS_CATALOG: List[CensusThemeSpec] = [
    # --------------------------------------------------------------------------
    # 2010 THEMES
    # --------------------------------------------------------------------------
    CensusThemeSpec(
        theme="basic",
        year=2010,
        strategy="bd_table",
        table_id=BD_TABLE_BASIC_2010,
        required_columns=["v001", "v002"],
    ),
    CensusThemeSpec(
        theme="income",
        year=2010,
        strategy="bd_table",
        table_id=BD_TABLE_BASIC_2010,
        required_columns=["v009"],
    ),
    CensusThemeSpec(
        theme="age",
        year=2010,
        strategy="bd_table",
        table_id=BD_TABLE_AGE_2010,
        # Fetch Total (v022) + all age ranges used in aggregation
        # Ranges derived from your original logic: 35-49, 49-54, 54-99, 99-135
        required_columns=["v022"] + _gen_cols("v", 35, 135, width=3),
    ),
    CensusThemeSpec(
        theme="race",
        year=2010,
        strategy="bd_table",
        table_id=BD_TABLE_RACE_2010,
        # v002=Branca, v003=Preta, v004=Amarela, v005=Parda, v006=Indigena
        required_columns=["v002", "v003", "v004", "v005", "v006"],
    ),
    # --------------------------------------------------------------------------
    # 2022 THEMES
    # --------------------------------------------------------------------------
    CensusThemeSpec(
        theme="basic",
        year=2022,
        strategy="bd_table",
        table_id=BD_TABLE_SETOR_2022,
        required_columns=["domicilios", "pessoas"],
    ),
    CensusThemeSpec(
        theme="basic",
        year=2022,
        strategy="ftp_csv",
        url=URL_BASIC_2022,
        column_map={
            "CD_SETOR": "id_setor_censitario",
            "v0001": "total_pessoas",
            "v0002": "total_domicilios",
            "v0003": "domicilios_particulares",
            "v0004": "domicilios_coletivos",
            "v0005": "media_moradores_dom_ocupados",
            "v0006": "pct_domicilios_imputados",
            "v0007": "domicilios_particulares_ocupados",
        },
    ),
    CensusThemeSpec(
        theme="income",
        year=2022,
        strategy="ftp_csv",
        url=URL_INCOME_2022,
        column_map={"CD_SETOR": "id_setor_censitario", "V06004": "rendimento_medio"},
        # No 'required_columns' here because the FTP adapter handles CSV parsing internally
    ),
    CensusThemeSpec(
        theme="age",
        year=2022,
        strategy="bd_table",
        table_id=BD_TABLE_SETOR_2022,
        # logic: Pessoas (Total) + V00644 (15-19) + 20-64 block + 65+ block
        required_columns=["pessoas", "V00644"] + _gen_cols("V", 645, 657, width=5),
    ),
    CensusThemeSpec(
        theme="race",
        year=2022,
        strategy="bd_table",
        table_id=BD_TABLE_SETOR_2022,
        # We need Total Pop, Pop 15+, and the Race breakdowns for 15+
        # Ranges: 644-657 (Pop 15+) and 657-717 (Race blocks)
        required_columns=["pessoas"]
        + _gen_cols("V", 644, 657, width=5)
        + _gen_cols("V", 657, 717, width=5),
    ),
    CensusThemeSpec(
        theme="race",
        year=2022,
        strategy="ftp_csv",
        url=URL_RACE_2022,
        column_map={
            "CD_SETOR": "id_setor_censitario",
            "V01317": "cor_ou_raca_branca",
            "V01318": "cor_ou_raca_preta",
            "V01319": "cor_ou_raca_amarela",
            "V01320": "cor_ou_raca_parda",
            "V01321": "cor_ou_raca_indigena",
        }
    )
]

# --- Accessor ---


def get_theme_spec(theme: str, year: int, strategy: str) -> CensusThemeSpec:
    """Retrieves the configuration for a given theme and year."""
    for spec in CENSUS_CATALOG:
        if spec.theme == theme and spec.year == year and spec.strategy == strategy:
            return spec
    raise ValueError(
        f"No configuration found in CENSUS_CATALOG for theme='{theme}', year={year}"
    )
