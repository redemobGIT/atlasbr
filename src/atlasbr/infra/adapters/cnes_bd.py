"""
AtlasBR - Infrastructure Adapter for CNES (Base dos Dados).
"""

import pandas as pd
import basedosdados as bd
from typing import Iterable, List
from atlasbr.core.catalog.cnes import CNES_INFRASTRUCTURE_GROUPS, CNES_UNIT_CODES
from atlasbr.settings import get_billing_id

def _build_infra_selects() -> str:
    """Helper to generate COALESCE sums for infrastructure groups."""
    selects = []
    for alias, cols in CNES_INFRASTRUCTURE_GROUPS.items():
        # SQL: COALESCE(col1, 0) + COALESCE(col2, 0) ... AS alias
        sum_expr = " + ".join(f"COALESCE({c}, 0)" for c in cols)
        selects.append(f"{sum_expr} AS {alias}")
    return ",\n        ".join(selects)

def fetch_cnes_from_bd(
    munis: Iterable[int],
    year: int,
    month: int,
    billing_id: str | None = None,
) -> pd.DataFrame:
    """
    Executes the complex CNES query including infrastructure aggregation
    and worker counting via BigQuery.
    """
    project_id = billing_id or settings.get_billing_id()

    muni_list_sql = ", ".join(f"'{int(m):07d}'" for m in munis)
    
    # Filter by specific unit types defined in catalog
    unit_codes = list(CNES_UNIT_CODES.keys())
    unit_list_sql = ", ".join(f"'{c}'" for c in unit_codes)
    
    # Generate dynamic parts of the query
    infra_sql = _build_infra_selects()
    
    # Identify tables (hardcoded here or passed from spec)
    table_estab = "basedosdados.br_ms_cnes.estabelecimento"
    table_prof = "basedosdados.br_ms_cnes.profissional"

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

                -- Complexity Logic (JSON Regex)
                CASE
                  WHEN REGEXP_CONTAINS(TO_JSON_STRING(e), r'"indicador_gestao_alta_[^"]+":\\s*1') THEN 'alta'
                  WHEN REGEXP_CONTAINS(TO_JSON_STRING(e), r'"indicador_gestao_media_[^"]+":\\s*1') THEN 'media'
                  WHEN REGEXP_CONTAINS(TO_JSON_STRING(e), r'"indicador_gestao_basica_[^"]+":\\s*1') THEN 'basica'
                END AS complexidade,

                -- Infrastructure Aggregations
                {infra_sql}

            FROM `{table_estab}` AS e
            WHERE e.id_municipio IN ({muni_list_sql})
              AND e.ano = {year}
              AND e.mes = {month}
              AND e.tipo_unidade IN ({unit_list_sql})
              AND e.tipo_pessoa = '3' -- Pessoa Jurídica (CNPJ)
        ),

        workers AS (
            SELECT
                t.id_estabelecimento_cnes,
                COALESCE(SUM(
                    CASE
                      WHEN SAFE_CAST(num AS INT64) = 88888 THEN 0
                      ELSE SAFE_CAST(num AS INT64)
                    END
                ), 0) AS quantidade_trabalhadores_saude
            FROM `{table_prof}` AS t
            LEFT JOIN UNNEST(
              REGEXP_EXTRACT_ALL(TO_JSON_STRING(t), r'"quantidade_profissional_[^"]+":\\s*([0-9]+)')
            ) AS num ON TRUE
            WHERE t.id_municipio IN ({muni_list_sql})
              AND t.ano = {year}
              AND t.mes = {month}
            GROUP BY t.id_estabelecimento_cnes
        )

        SELECT
            e.*,
            COALESCE(w.quantidade_trabalhadores_saude, 0) AS quantidade_trabalhadores_saude
        FROM estab AS e
        LEFT JOIN workers AS w USING (id_estabelecimento_cnes)
    """
    
    print(f"    🏥 Fetching CNES {month}/{year} from Base dos Dados...")
    return bd.read_sql(query, billing_project_id=project_id)