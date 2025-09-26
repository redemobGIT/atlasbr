from typing import Tuple, Dict
import pandas as pd

# =============================================================================
# CONSTANTES E MAPEAMENTOS
# =============================================================================

# Mapeamento oficial de Seções CNAE → intervalos de divisão
CNAE_SECTIONS: list[Tuple[str, int, int]] = [
    ("A",   1,   3),   # Agricultura, Pecuária, Produção Florestal, Pesca e Aqüicultura
    ("B",   5,   9),   # Indústrias Extrativas
    ("C",  10,  33),   # Indústrias de Transformação
    ("D",  35,  35),   # Eletricidade e Gás
    ("E",  36,  39),   # Água, Esgoto, Ativ. de Gestão de Resíduos e Descontaminação
    ("F",  41,  43),   # Construção
    ("G",  45,  47),   # Comércio; Reparação de Veículos Automotores e Moticletas
    ("H",  49,  53),   # Transporte, Armazenagem e Correio
    ("I",  55,  56),   # Alojamento e Alimentação
    ("J",  58,  63),   # Informação e Comunicação
    ("K",  64,  66),   # Atividades Financeiras, de Seguros e Serviços Relacionados
    ("L",  68,  68),   # Atividades Imobiliárias
    ("M",  69,  75),   # Atividades Profissionais, Científicas e Técnicas
    ("N",  77,  82),   # Atividades Administrativas e Serviços Complementares
    ("O",  84,  84),   # Administração Pública, Defesa e Seguridade Social
    ("P",  85,  85),   # Educação
    ("Q",  86,  88),   # Saúde Humana e Serviços Sociais
    ("R",  90,  93),   # Artes, Cultura, Esporte e Recreação
    ("S",  94,  96),   # Outras Atividades de Serviços
    ("T",  97,  97),   # Serviços Domésticos
    ("U",  99,  99),   # Organismos Internacionais e Outras Instituições Extraterritoriais
]

# Mapeamento da letra da Seção CNAE para um nome descritivo do setor
CNAE_SECTORS: Dict[str, str] = {
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

# Prefixos CNAE cujo número de vínculos pode ser limitado em análises
CNAE_PROBLEM_PREFIXES: list[str] = [
    '35', '36', '38', '41', '42', '43', '49', '51', '562', '64', '78',
    '80', '81', '82', '84',
]

# Expressão regular derivada dos prefixos problemáticos
CNAE_PROBLEM_REGEX: str = rf"^({'|'.join(CNAE_PROBLEM_PREFIXES)})"

# Pré-cálculo do Mapa de Prefixos para performance
_CNAE_PREFIX_TO_SECTION_MAP: Dict[str, str] = {
    f"{i:02d}": letter
    for letter, start, end in CNAE_SECTIONS
    for i in range(start, end + 1)
}


# =============================================================================
# FUNÇÕES DE PROCESSAMENTO
# =============================================================================

def add_cnae_section_letter(
    df: pd.DataFrame,
    cnae_col: str = "cnae_2",
    section_col: str = "cnae_section_letter"
) -> pd.DataFrame:
    """Adiciona uma coluna com a letra da Seção CNAE correspondente."""
    if cnae_col not in df.columns:
        raise ValueError(f"A coluna '{cnae_col}' não foi encontrada no DataFrame.")

    cnae_prefixes = df[cnae_col].astype(str).str.zfill(7).str[:2]
    df[section_col] = cnae_prefixes.map(_CNAE_PREFIX_TO_SECTION_MAP)
    return df

def add_cnae_sector_name(
    df: pd.DataFrame,
    section_letter_col: str = "cnae_section_letter",
    sector_name_col: str = "cnae_sector_name"
) -> pd.DataFrame:
    """Adiciona uma coluna com o nome descritivo do setor CNAE."""
    if section_letter_col not in df.columns:
        raise ValueError(
            f"A coluna '{section_letter_col}' não foi encontrada. "
            f"Execute a função 'add_cnae_section_letter' primeiro."
        )

    df[sector_name_col] = df[section_letter_col].map(CNAE_SECTORS)
    return df