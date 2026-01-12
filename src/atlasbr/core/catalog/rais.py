"""
AtlasBR - Core Catalog for RAIS Data.

Defines the contract for fetching RAIS data and the taxonomies for CNAE codes.
"""

from __future__ import annotations

from typing import Dict, List, Literal, Optional, Tuple
from warnings import warn

from pydantic import BaseModel, ConfigDict, Field

RaisStrategy = Literal[bd, "ftp"]

CNAE_SECTIONS_DEF: List[Tuple[str, int, int]] = [
    ("A", 1, 3),
    ("B", 5, 9),
    ("C", 10, 33),
    ("D", 35, 35),
    ("E", 36, 39),
    ("F", 41, 43),
    ("G", 45, 47),
    ("H", 49, 53),
    ("I", 55, 56),
    ("J", 58, 63),
    ("K", 64, 66),
    ("L", 68, 68),
    ("M", 69, 75),
    ("N", 77, 82),
    ("O", 84, 84),
    ("P", 85, 85),
    ("Q", 86, 88),
    ("R", 90, 93),
    ("S", 94, 96),
    ("T", 97, 97),
    ("U", 99, 99),
]

CNAE_SECTOR_NAMES: Dict[str, str] = {
    "A": "Agricultura e Pesca",
    "B": "Indústrias Extrativas",
    "C": "Indústrias de Transformação",
    "D": "Eletricidade e Gás",
    "E": "Água e Gestão de Resíduos",
    "F": "Construção",
    "G": "Comércio e Reparação de Veículos",
    "H": "Transporte e Armazenagem",
    "I": "Alojamento e Alimentação",
    "J": "Informação e Comunicação",
    "K": "Finanças e Seguros",
    "L": "Atividades Imobiliárias",
    "M": "Serviços Profissionais e Técnicos",
    "N": "Serviços Administrativos",
    "O": "Administração Pública",
    "P": "Educação",
    "Q": "Saúde e Assistência Social",
    "R": "Artes e Recreação",
    "S": "Outros Serviços",
    "T": "Serviços Domésticos",
    "U": "Organizações Internacionais",
}

CNAE_PROBLEM_PREFIXES: List[str] = [
    "35",
    "36",
    "38",
    "41",
    "42",
    "43",
    "49",
    "51",
    "562",
    "64",
    "78",
    "80",
    "81",
    "82",
    "84",
]


class RaisThemeSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy: RaisStrategy
    table_id: Optional[str] = None
    required_columns: List[str] = Field(default_factory=list)

    year: Optional[int] = None


RAIS_CATALOG: List[RaisThemeSpec] = [
    RaisThemeSpec(
        strategy="bd",
        table_id="basedosdados.br_me_rais.microdados_estabelecimentos",
        required_columns=[
            "id_municipio",
            "tipo_estabelecimento",
            "cnae_2",
            "quantidade_vinculos_ativos",
            "cep",
            "natureza_juridica",
        ],
    ),
]


def get_rais_spec(year: int, strategy: str) -> RaisThemeSpec:
    for spec in RAIS_CATALOG:
        if spec.strategy != strategy:
            continue
        if spec.year is None or spec.year == year:
            if spec.year is None:
                warn(
                    "RAIS catalog is year-agnostic for BD; pass year to "
                    "load/fetch functions for filtering.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            return spec

    raise ValueError(
        f"No RAIS configuration found for year={year}, strategy={strategy!r}."
    )
