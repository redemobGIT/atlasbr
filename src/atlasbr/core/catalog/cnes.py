"""
AtlasBR - Core Catalog for CNES (Healthcare) Data.

Defines infrastructure groups, unit codes, and the fetching contract.
"""

from typing import Literal, List, Dict, Optional
from pydantic import BaseModel, Field

# --- Constants (Taxonomy) ---

# Map Unit Code -> Description (for reference or filtering)
CNES_UNIT_CODES: Dict[str, str] = {
    "1": "Hospital Geral",
    "2": "Hospital Especializado",
    "3": "Hospital-Dia",
    "4": "Unidade Mista",
    "5": "Unidade Básica de Saúde",
    "6": "Posto de Saúde",
    "7": "Centro de Saúde/Unidade Básica",
    "8": "Policlínica",
    "9": "Unidade Móvel Terrestre",
    "10": "Pronto Socorro Geral",
    "11": "Pronto Socorro Especializado",
    "12": "Unidade de Apoio à Diagnose e Terapia",
    "13": "Unidade de Atenção Especializada Ambulatorial",
    "14": "Centro de Parto Normal - Isolado",
    "15": "Centro de Atenção Hemoterapia e Hematologia",
    "16": "Centro de Atenção à Saúde Auditiva",
    "17": "Centro de Especialidades Odontológicas",
    "18": "Centro de Reabilitação",
    "19": "Hospital de Ensino",
    "20": "Unidade de Pronto Atendimento (UPA 24h)",
    "21": "Centro de Saúde Escola",
}

# Group raw columns into semantic concepts
CNES_INFRASTRUCTURE_GROUPS: Dict[str, List[str]] = {
    "total_leitos_internacao": [
        "quantidade_leito_clinico",
        "quantidade_leito_cirurgico",
        "quantidade_leito_complementar",
    ],
    "total_leitos_observacao": [
        "quantidade_leito_repouso_pediatrico_urgencia",
        "quantidade_leito_repouso_feminino_urgencia",
        "quantidade_leito_repouso_masculino_urgencia",
        "quantidade_leito_repouso_indiferenciado_urgencia",
        "quantidade_leito_repouso_feminino_ambulatorial",
        "quantidade_leito_repouso_masculino_ambulatorial",
        "quantidade_leito_repouso_pediatrico_ambulatorial",
        "quantidade_leito_repouso_indiferenciado_ambulatorial",
        "quantidade_leito_recuperacao_centro_cirurgico",
    ],
    "total_leitos_materno_infantil": [
        "quantidade_leito_pre_parto_centro_obstetrico",
        "quantidade_leito_recem_nascido_normal_neonatal",
        "quantidade_leito_recem_nascido_patologico_neonatal",
        "quantidade_leito_conjunto_neonatal",
    ],
    "total_consultorios": [
        "quantidade_consultorio_medico_urgencia",
        "quantidade_consultorio_pediatrico_urgencia",
        "quantidade_consultorio_feminino_urgencia",
        "quantidade_consultorio_masculino_urgencia",
        "quantidade_consultorio_indiferenciado_urgencia",
        "quantidade_consultorio_odontologia_urgencia",
        "quantidade_consultorio_clinica_basica_ambulatorial",
        "quantidade_consultorio_clinica_especializada_ambulatorial",
        "quantidade_consultorio_clinica_indiferenciada_ambulatorial",
        "quantidade_consultorio_nao_medico_ambulatorial",
        "quantidade_consultorio_odontologia_ambulatorial",
    ],
    "total_salas_procedimentos": [
        "quantidade_sala_curativo_urgencia",
        "quantidade_sala_gesso_urgencia",
        "quantidade_sala_pequena_cirurgia_urgencia",
        "quantidade_sala_higienizacao_urgencia",
        "quantidade_sala_curativo_ambulatorial",
        "quantidade_sala_gesso_ambulatorial",
        "quantidade_sala_pequena_cirurgia_ambulatorial",
        "quantidade_sala_enfermagem_ambulatorial",
        "quantidade_sala_imunizacao_ambulatorial",
        "quantidade_sala_nebulizacao_ambulatorial",
    ],
    "total_salas_cirurgicas_obstetricas": [
        "quantidade_sala_cirurgia_ambulatorial",
        "quantidade_sala_cirurgia_ambulatorial_centro_cirurgico",
        "quantidade_sala_cirurgia_centro_cirurgico",
        "quantidade_sala_recuperacao_centro_cirurgico",
        "quantidade_sala_pre_parto_centro_obstetrico",
        "quantidade_sala_parto_normal_centro_obstetrico",
        "quantidade_sala_curetagem_centro_obstetrico",
        "quantidade_sala_cirurgia_centro_obstetrico",
    ],
}

# --- Domain Spec ---

class CnesThemeSpec(BaseModel):
    year: int
    month: int
    strategy: Literal["bd_complex_sql"] # SQL is too complex to split cleanly right now
    table_estab: str = "basedosdados.br_ms_cnes.estabelecimento"
    table_prof: str = "basedosdados.br_ms_cnes.profissional"

    class Config:
        frozen = True

def get_cnes_spec(year: int, month: int) -> CnesThemeSpec:
    return CnesThemeSpec(year=year, month=month, strategy="bd_complex_sql")