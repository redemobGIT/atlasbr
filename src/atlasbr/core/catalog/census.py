"""
AtlasBR - Core Catalog for Census Data.

Defines the "contract" for every Census theme and strategy (BigQuery vs. FTP).
Fully supports 2010 and 2022 demographic, economic, and racial data.
"""

from typing import Literal, List, Dict, Optional, Tuple, TypeAlias
from pydantic import BaseModel, Field, ConfigDict

# ---------------------------------------------------------------------
# Type Definitions
# ---------------------------------------------------------------------

CensusStrategy: TypeAlias = Literal["bd_table", "ftp_csv"]

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _gen_cols(prefix: str, start: int, end: int, width: int = 3) -> List[str]:
    """Generates a sequence of column names (e.g., v035, v036...)."""
    return [f"{prefix}{i:0{width}d}" for i in range(start, end)]


# ---------------------------------------------------------------------
# Constants (BD tables)
# ---------------------------------------------------------------------

BD_TABLE_BASIC_2010 = (
    "basedosdados.br_ibge_censo_demografico.setor_censitario_basico_2010"
)
BD_TABLE_AGE_2010 = (
    "basedosdados.br_ibge_censo_demografico.setor_censitario_idade_total_2010"
)
BD_TABLE_RACE_2010 = (
    "basedosdados.br_ibge_censo_demografico."
    "setor_censitario_raca_idade_genero_2010"
)
BD_TABLE_SETOR_2022 = (
    "basedosdados.br_ibge_censo_2022.populacao_domicilios"
)

# ---------------------------------------------------------------------
# Constants (FTP URLs)
# ---------------------------------------------------------------------

# 2010 Root: We use {uf} placeholders for state-specific zips
FTP_DIR_2010 = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/"
    "Resultados_do_Universo/Agregados_por_Setores_Censitarios/"
)

# 2010 ZIPs are published as <STEM>_YYYYMMDD.zip (revision date varies).
FTP_TEMPLATE_2010 = FTP_DIR_2010 + "{stem}.zip"

# 2022 Roots
FTP_ROOT_2022 = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Agregados_por_Setores_Censitarios"
)

# 2022 Files (National Level)
URL_BASIC_2022 = (
    f"{FTP_ROOT_2022}/Agregados_por_Setor_csv/"
    "Agregados_por_setores_basico_BR_20250417.zip"
)
URL_INCOME_2022 = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Agregados_por_Setores_Censitarios_Rendimento_do_Responsavel/"
    "Agregados_por_setores_renda_responsavel_BR_csv.zip"
)
URL_RACE_2022 = (
    f"{FTP_ROOT_2022}/Agregados_por_Setor_csv/"
    "Agregados_por_setores_cor_ou_raca_BR.zip"
)
URL_AGE_2022 = (
    f"{FTP_ROOT_2022}/Agregados_por_Setor_csv/"
    "Agregados_por_setores_alfabetizacao_BR.zip"
)

# ---------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------


class FtpResourceSpec(BaseModel):
    """
    Represents one physical CSV resource, possibly inside a ZIP.
    """
    url_template: str
    member_glob: Optional[str] = None
    encoding: str = "latin1"
    sep: str = ";"
    id_col: str = "Cod_setor"


class CensusThemeSpec(BaseModel):
    """
    Defines the configuration contract for a Census theme.
    """
    model_config = ConfigDict(frozen=True)

    theme: str
    year: int
    strategy: CensusStrategy

    # --- BD strategy ---
    table_id: Optional[str] = None
    required_columns: List[str] = Field(default_factory=list)

    # --- FTP strategy ---
    ftp_resources: List[FtpResourceSpec] = Field(default_factory=list)
    column_map: Dict[str, str] = Field(default_factory=dict)

    # --- H3 Aggregation Metadata ---
    # Variables to SUM (counts) vs AVERAGE (densities/rates)
    extensive_vars: List[str] = Field(default_factory=list)
    intensive_vars: List[str] = Field(default_factory=list)


# ---------------------------------------------------------------------
# Catalog Registry
# ---------------------------------------------------------------------

CENSUS_CATALOG: List[CensusThemeSpec] = [

    # ==========================================
    # 2010 CENSUS
    # ==========================================

    # --- 2010 BD ---
    CensusThemeSpec(
        theme="basic", year=2010, strategy="bd_table",
        table_id=BD_TABLE_BASIC_2010,
        required_columns=["v001", "v002"],
        column_map={
            "id_setor_censitario": "id_setor_censitario",
            "v002": "habitantes",
        },
        extensive_vars=["habitantes"]
    ),
    CensusThemeSpec(
        theme="income", year=2010, strategy="bd_table",
        table_id=BD_TABLE_BASIC_2010,
        required_columns=["v005"],
        column_map={
            "id_setor_censitario": "id_setor_censitario",
            "v005": "rendimento_medio",
        },
        intensive_vars=["rendimento_medio"]
    ),
    CensusThemeSpec(
        theme="age", year=2010, strategy="bd_table",
        table_id=BD_TABLE_AGE_2010,
        required_columns=["v022"] + _gen_cols("v", 35, 135),
        extensive_vars=["v022"] + _gen_cols("v", 35, 135),
    ),
    CensusThemeSpec(
        theme="race", year=2010, strategy="bd_table",
        table_id=BD_TABLE_RACE_2010,
        required_columns=["v002", "v003", "v004", "v005", "v006"],
        column_map={
            "id_setor_censitario": "id_setor_censitario",
            "v002": "cor_branca",
            "v003": "cor_preta",
            "v004": "cor_amarela",
            "v005": "cor_parda",
            "v006": "cor_indigena",
        },
        extensive_vars=[
            "cor_branca", "cor_preta", "cor_amarela",
            "cor_parda", "cor_indigena"
        ]
    ),

    # --- 2010 FTP ---
    CensusThemeSpec(
        theme="basic", year=2010, strategy="ftp_csv",
        ftp_resources=[
            FtpResourceSpec(
                url_template=FTP_TEMPLATE_2010,
                member_glob="Basico_*.csv",
                id_col="Cod_setor"
            )
        ],
        column_map={
            "Cod_setor": "id_setor_censitario",
            "V002": "habitantes"
        },
        extensive_vars=["habitantes"]
    ),
    CensusThemeSpec(
        theme="income", year=2010, strategy="ftp_csv",
        ftp_resources=[
            FtpResourceSpec(
                url_template=FTP_TEMPLATE_2010,
                member_glob="Basico_*.csv",
                id_col="Cod_setor"
            )
        ],
        column_map={
            "Cod_setor": "id_setor_censitario",
            "V005": "rendimento_medio"
        },
        intensive_vars=["rendimento_medio"]
    ),
    CensusThemeSpec(
        theme="race", year=2010, strategy="ftp_csv",
        ftp_resources=[
            FtpResourceSpec(
                url_template=FTP_TEMPLATE_2010,
                member_glob="Pessoa03_*.csv",
                id_col="Cod_setor"
            )
        ],
        column_map={
            "Cod_setor": "id_setor_censitario",
            "V002": "cor_branca",
            "V003": "cor_preta",
            "V004": "cor_amarela",
            "V005": "cor_parda",
            "V006": "cor_indigena"
        },
        extensive_vars=[
            "cor_branca", "cor_preta", "cor_amarela",
            "cor_parda", "cor_indigena"
        ]
    ),
    # [NEW] 2010 Age FTP
    CensusThemeSpec(
        theme="age", year=2010, strategy="ftp_csv",
        ftp_resources=[
            FtpResourceSpec(
                url_template=FTP_TEMPLATE_2010.format(stem="Pessoa11"),
                member_glob="Pessoa11_*.csv",
                id_col="Cod_setor"
            )
        ],
        required_columns=["V022"] + _gen_cols("V", 35, 135),
        column_map={
            "Cod_setor": "id_setor_censitario",
            "V022": "v022",
            **{f"V{i:03d}": f"v{i:03d}" for i in range(35, 135)},
        },
        extensive_vars=["v022"] + _gen_cols("v", 35, 135),
    ),

    # ==========================================
    # 2022 CENSUS
    # ==========================================

    # --- 2022 BD ---
    CensusThemeSpec(
        theme="basic", year=2022, strategy="bd_table",
        table_id=BD_TABLE_SETOR_2022,
        required_columns=["domicilios", "pessoas"],
        column_map={
            "id_setor_censitario": "id_setor_censitario",
            "pessoas": "habitantes",
            "domicilios": "total_domicilios"
        },
        extensive_vars=["habitantes", "total_domicilios"]
    ),
    CensusThemeSpec(
        theme="age", year=2022, strategy="bd_table",
        table_id=BD_TABLE_SETOR_2022,
        # Logic: Pessoas (Total) + V00644 (15-19) + 20-64 block + 65+ block
        required_columns=(
            ["pessoas", "V00644"] + _gen_cols("V", 645, 657, width=5)
        ),
        extensive_vars=(
            ["pessoas", "V00644"] + _gen_cols("V", 645, 657, width=5)
        ),
    ),
    CensusThemeSpec(
        theme="race", year=2022, strategy="bd_table",
        table_id=BD_TABLE_SETOR_2022,
        # Logic: 644-657 (Pop 15+) and 657-717 (Race breakdowns)
        required_columns=(
            ["pessoas"] +
            _gen_cols("V", 644, 657, width=5) +
            _gen_cols("V", 657, 717, width=5)
        ),
        extensive_vars=(
            ["pessoas"] +
            _gen_cols("V", 644, 657, width=5) +
            _gen_cols("V", 657, 717, width=5)
        ),
    ),

    # --- 2022 FTP ---
    CensusThemeSpec(
        theme="basic", year=2022, strategy="ftp_csv",
        ftp_resources=[
            FtpResourceSpec(
                url_template=URL_BASIC_2022,
                member_glob="*.csv",
                id_col="CD_SETOR"
            )
        ],
        column_map={
            "CD_SETOR": "id_setor_censitario",
            "v0001": "habitantes",
            "v0002": "total_domicilios",
            "v0003": "domicilios_particulares",
            "v0004": "domicilios_coletivos",
            "v0005": "media_moradores_dom_ocupados",
            "v0006": "pct_domicilios_imputados",
            "v0007": "domicilios_particulares_ocupados",
        },
        extensive_vars=[
            "habitantes", "total_domicilios",
            "domicilios_particulares", "domicilios_coletivos",
            "domicilios_particulares_ocupados"
        ],
        intensive_vars=[
            "media_moradores_dom_ocupados", "pct_domicilios_imputados"
        ]
    ),
    CensusThemeSpec(
        theme="income", year=2022, strategy="ftp_csv",
        ftp_resources=[
            FtpResourceSpec(
                url_template=URL_INCOME_2022,
                member_glob="*.csv",
                id_col="CD_SETOR"
            )
        ],
        column_map={
            "CD_SETOR": "id_setor_censitario",
            "V06004": "rendimento_medio"
        },
        intensive_vars=["rendimento_medio"]
    ),
    CensusThemeSpec(
        theme="race", year=2022, strategy="ftp_csv",
        ftp_resources=[
            FtpResourceSpec(
                url_template=URL_RACE_2022,
                member_glob="*.csv",
                id_col="CD_SETOR"
            )
        ],
        column_map={
            "CD_SETOR": "id_setor_censitario",
            "V01317": "cor_branca",
            "V01318": "cor_preta",
            "V01319": "cor_amarela",
            "V01320": "cor_parda",
            "V01321": "cor_indigena",
        },
        extensive_vars=[
            "cor_branca", "cor_preta", "cor_amarela",
            "cor_parda", "cor_indigena"
        ]
    ),
    # [NEW] 2022 Age FTP
    CensusThemeSpec(
        theme="age", year=2022, strategy="ftp_csv",
        ftp_resources=[
            FtpResourceSpec(
                url_template=URL_AGE_2022,
                member_glob="Agregados_por_setores_alfabetizacao_BR.csv",
                id_col="CD_SETOR",
            )
        ],
        # Explicitly excluding V00748 (literacy) to avoid double counting
        required_columns=[
            "V00644", "V00649", "V00654", "V00659", "V00664",
            "V00669", "V00674", "V00679"
        ],
        column_map={"CD_SETOR": "id_setor_censitario"},
        extensive_vars=[
            "V00644", "V00649", "V00654", "V00659", "V00664",
            "V00669", "V00674", "V00679"
        ],
    ),
]

# ---------------------------------------------------------------------
# Lookup Logic
# ---------------------------------------------------------------------

_CATALOG_INDEX: Dict[
    Tuple[str, int, CensusStrategy], CensusThemeSpec
] = {
    (spec.theme, spec.year, spec.strategy): spec
    for spec in CENSUS_CATALOG
}


def get_census_spec(year: int, theme: str, strategy: str) -> CensusThemeSpec:
    """Retrieve a CensusThemeSpec by (theme, year, strategy)."""
    key = (theme, year, strategy)

    if key in _CATALOG_INDEX:
        return _CATALOG_INDEX[key]

    available: List[str] = sorted({
        t for (t, y, s) in _CATALOG_INDEX.keys()
        if y == year and s == strategy
    })

    if available:
        raise ValueError(
            f"No catalog entry found for Census {year} ('{theme}') "
            f"using '{strategy}'. "
            f"Available themes for {year}/{strategy}: {available}"
        )

    raise ValueError(
        f"No catalog entry found for Census {year} ('{theme}') "
        f"using '{strategy}'. "
        "No themes are registered for this year/strategy combination."
    )