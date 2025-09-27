from typing import Sequence, Iterable
import pandas as pd
import basedosdados as bd

from roda.utils.sql import sql_list
from roda.utils.cnae import CNAE_PROBLEM_REGEX

def fetch_rais(
    muni_ids: Sequence[int | str],
    years: Sequence[int] | int = 2022,
    *,
    billing_project_id: str | None = None,
) -> pd.DataFrame:
    """
    Extrai dados da RAIS com limite adaptativo para CNAEs problemáticos.
    """
    mun_sql = sql_list(muni_ids, quote=True, pad=7)
    year_seq = years if isinstance(years, Iterable) else [years]
    yr_sql = sql_list(year_seq)
    
    # Passa a constante do regex para a query
    _RX_PROBLEM = CNAE_PROBLEM_REGEX

    query = f"""
            WITH base AS (
                SELECT
                    ano,
                    id_municipio,
                    tipo_estabelecimento,
                    cnae_2,
                    quantidade_vinculos_ativos AS jobs_raw,
                    cep,
                    REGEXP_EXTRACT(cnae_2, r'^(\\d{{2,3}})') AS prefix
                FROM `basedosdados.br_me_rais.microdados_estabelecimentos`
                WHERE ano IN ({yr_sql})
                    AND id_municipio IN ({mun_sql})
                    AND NOT (LEFT(natureza_juridica, 1) = '1'
                            OR natureza_juridica = '2011')
            ),

            stats AS (
                SELECT DISTINCT
                    prefix,
                    PERCENTILE_CONT(jobs_raw, 0.95)
                    OVER (PARTITION BY prefix)     AS p95
                FROM base
            )

            SELECT
                b.ano,
                b.id_municipio,
                b.tipo_estabelecimento,
                b.cnae_2,
                CAST(
                CASE
                    WHEN REGEXP_CONTAINS(b.cnae_2, r'{_RX_PROBLEM}')
                        AND COALESCE(b.jobs_raw, 0) > COALESCE(s.p95, 0)
                    THEN COALESCE(s.p95, 0)
                    ELSE COALESCE(b.jobs_raw, 0)
                END AS INT64
                ) AS quantidade_vinculos_ativos,
                b.cep
            FROM base  AS b
            JOIN stats AS s USING (prefix)
        """
    return bd.read_sql(query, billing_project_id=billing_project_id)