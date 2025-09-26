"""
Módulo para coleta e processamento de dados do Cadastro Nacional de
Estabelecimentos de Saúde (CNES) a partir da plataforma Base dos Dados.
"""

from typing import Sequence, Dict
import pandas as pd
import basedosdados as bd

# Importa utilitários de outros módulos da biblioteca
from roda.utils.sql import sql_list

# =============================================================================
# CONSTANTES E MAPEAMENTOS ESPECÍFICOS DO CNES
# =============================================================================

# Tipos de estabelecimento CNES selecionados para análise
CNES_UNIT_CODES: list[str] = [
    "1",   # Hospital Geral
    "2",   # Hospital Especializado
    "3",   # Hospital-Dia
    "4",   # Unidade Mista
    "5",   # Unidade Básica de Saúde
    "6",   # Posto de Saúde
    "7",   # Centro de Saúde/Unidade Básica
    "8",   # Policlínica
    "9",   # Unidade Móvel Terrestre
    "10",  # Pronto Socorro Geral
    "11",  # Pronto Socorro Especializado
    "12",  # Unidade de Apoio à Diagnose e Terapia
    "13",  # Unidade de Atenção Especializada Ambulatorial
    "14",  # Centro de Parto Normal - Isolado
    "15",  # Centro de Atenção Hemoterapia e Hematologia
    "16",  # Centro de Atenção à Saúde Auditiva
    "17",  # Centro de Especialidades Odontológicas
    "18",  # Centro de Reabilitação
    "19",  # Hospital de Ensino
    "20",  # Unidade de Pronto Atendimento (UPA 24h)
    "21",  # Centro de Saúde Escola
]

# Tipos de estabelecimento CNES selecionados para análise
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

# Mapeamento de grupos de infraestrutura para colunas específicas do CNES
CNES_INFRASTRUCTURE_GROUPS: Dict[str, list[str]] = {
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


# =============================================================================
# FUNÇÕES DE COLETA DE DADOS
# =============================================================================

def _group_expressions() -> str:
    """
    Função auxiliar interna para compor as somas com COALESCE para os grupos
    de infraestrutura do CNES.
    """
    out: list[str] = []
    # Agora usa a constante definida neste mesmo arquivo
    for alias, cols in CNES_INFRASTRUCTURE_GROUPS.items():
        summands = " + ".join(f"COALESCE({c},0)" for c in cols)
        out.append(f"{summands} AS {alias}")
    return ",\n                      ".join(out)

def fetch_healthcare(
    muni_ids: Sequence[int | str],
    *,
    ano: int = 2023,
    mes: int = 9,
    billing_project_id: str | None = None,
) -> pd.DataFrame:
    """
    Busca estabelecimentos CNES enriquecidos com somas de infraestrutura.
    """
    muni_sql = sql_list(muni_ids, quote=True, pad=7)
    # Usa as constantes definidas neste mesmo arquivo
    unit_sql = sql_list(CNES_UNIT_CODES, quote=True)
    infra_sql = _group_expressions()

    query = f"""
        WITH estab AS (
            SELECT
                e.id_estabelecimento_cnes,
                e.id_municipio,
                e.cep,
                e.tipo_unidade,
                e.tipo_pessoa,
                e.indicador_vinculo_sus,
                e.indicador_atencao_hospitalar,

                CASE
                  WHEN REGEXP_CONTAINS(
                          TO_JSON_STRING(e),
                          r'"indicador_gestao_alta_[^"]+":\\s*1')
                  THEN 'alta'
                  WHEN REGEXP_CONTAINS(
                          TO_JSON_STRING(e),
                          r'"indicador_gestao_media_[^"]+":\\s*1')
                  THEN 'media'
                  WHEN REGEXP_CONTAINS(
                          TO_JSON_STRING(e),
                          r'"indicador_gestao_basica_[^"]+":\\s*1')
                  THEN 'basica'
                END AS complexidade,

                {infra_sql}

            FROM `basedosdados.br_ms_cnes.estabelecimento` AS e
            WHERE e.id_municipio IN ({muni_sql})
              AND e.ano = {ano}
              AND e.mes = {mes}
              AND e.tipo_unidade IN ({unit_sql})
              AND e.tipo_pessoa = '3' -- Pessoa Jurídica (CNPJ)
        ),

        workers AS (
            SELECT
                t.id_estabelecimento_cnes,
                COALESCE(
                  SUM(
                    CASE
                      WHEN SAFE_CAST(num AS INT64) = 88888 THEN 0
                      ELSE SAFE_CAST(num AS INT64)
                    END
                  ),
                  0
                ) AS quantidade_trabalhadores_saude
            FROM `basedosdados.br_ms_cnes.profissional` AS t
            LEFT JOIN UNNEST(
              REGEXP_EXTRACT_ALL(
                TO_JSON_STRING(t),
                r'"quantidade_profissional_[^"]+":\\s*([0-9]+)'
              )
            ) AS num ON TRUE
            WHERE t.id_municipio IN ({muni_sql})
              AND t.ano = {ano}
              AND t.mes = {mes}
            GROUP BY t.id_estabelecimento_cnes
        )

        SELECT
            e.*,
            COALESCE(w.quantidade_trabalhadores_saude, 0)
              AS quantidade_trabalhadores_saude
        FROM estab   AS e
        LEFT JOIN workers AS w USING (id_estabelecimento_cnes)
    """
    return bd.read_sql(query, billing_project_id=billing_project_id)