"""
AtlasBR - Core Catalog for RAIS Data.

Defines the contract for fetching RAIS data and the taxonomies for CNAE codes.
"""

from typing import Literal, List, Tuple, Dict, Optional
from pydantic import BaseModel, Field

# --- CNAE Constants ---

# Official mapping: (Section Letter, Start Division, End Division)
CNAE_SECTIONS_DEF: List[Tuple[str, int, int]] = [
    ("A", 1, 3), ("B", 5, 9), ("C", 10, 33), ("D", 35, 35),
    ("E", 36, 39), ("F", 41, 43), ("G", 45, 47), ("H", 49, 53),
    ("I", 55, 56), ("J", 58, 63), ("K", 64, 66), ("L", 68, 68),
    ("M", 69, 75), ("N", 77, 82), ("O", 84, 84), ("P", 85, 85),
    ("Q", 86, 88), ("R", 90, 93), ("S", 94, 96), ("T", 97, 97),
    ("U", 99, 99),
]

CNAE_SECTOR_NAMES: Dict[str, str] = {
    "A": "Agricultura e Pesca", "B": "Indústrias Extrativas",
    "C": "Indústrias de Transformação", "D": "Eletricidade e Gás",
    "E": "Água e Gestão de Resíduos", "F": "Construção",
    "G": "Comércio e Reparação de Veículos", "H": "Transporte e Armazenagem",
    "I": "Alojamento e Alimentação", "J": "Informação e Comunicação",
    "K": "Finanças e Seguros", "L": "Atividades Imobiliárias",
    "M": "Serviços Profissionais e Técnicos", "N": "Serviços Administrativos",
    "O": "Administração Pública", "P": "Educação",
    "Q": "Saúde e Assistência Social", "R": "Artes e Recreação",
    "S": "Outros Serviços", "T": "Serviços Domésticos",
    "U": "Organizações Internacionais",
}

# Prefixes for jobs that are likely headquarter-assigned rather than local
CNAE_PROBLEM_PREFIXES: List[str] = [
    '35', '36', '38', '41', '42', '43', '49', '51', '562', 
    '64', '78', '80', '81', '82', '84',
]

# --- Domain Models ---

class RaisThemeSpec(BaseModel):
    year: int
    strategy: Literal["bd_table", "ftp_csv"]
    
    # For Base dos Dados
    table_id: Optional[str] = None
    required_columns: List[str] = Field(default_factory=list)

    class Config:
        frozen = True

# --- Registry ---

RAIS_CATALOG: List[RaisThemeSpec] = [
    RaisThemeSpec(
        year=2022, 
        strategy="bd_table",
        table_id="basedosdados.br_me_rais.microdados_estabelecimentos",
        required_columns=[
            "id_municipio", 
            "tipo_estabelecimento", 
            "cnae_2", 
            "quantidade_vinculos_ativos", 
            "cep", 
            "natureza_juridica"
        ]
    )
]

def get_rais_spec(year: int) -> RaisThemeSpec:
    return RaisThemeSpec(
        year=year,
        strategy="bd_table",
        table_id="basedosdados.br_me_rais.microdados_estabelecimentos",
        required_columns=[
            "id_municipio",
            "tipo_estabelecimento", 
            "cnae_2", 
            "quantidade_vinculos_ativos", 
            "cep", 
            "natureza_juridica"
        ]
    )