from typing import Sequence
import pandas as pd
import basedosdados as bd

from roda.utils.sql import sql_list

def fetch_schools(
    muni_ids: Sequence[int | str],
    *,
    year_censo: int = 2023,
    billing_project_id: str | None = None,
) -> pd.DataFrame:
    """
    Busca dados do Censo Escolar para os municípios especificados.
    """
    muni_ids_sql = sql_list(muni_ids, quote=True, pad=7)
    query = f"""
            WITH dir AS (
                SELECT
                    id_escola,
                    id_municipio,
                    dependencia_administrativa,
                    etapas_modalidades_oferecidas,
                    endereco,
                    latitude,
                    longitude
                FROM `basedosdados.br_bd_diretorios_brasil.escola`
                WHERE id_municipio IN ({muni_ids_sql})
                  AND latitude  IS NOT NULL
                  AND longitude IS NOT NULL
            ),

            cen AS (
                SELECT
                    t.id_escola,
                    CASE
                      WHEN CAST(t.rede AS STRING) IN ('1','2','3') THEN 'Publica'
                      ELSE 'Privada'
                    END                                         AS rede,
                    t.quantidade_matricula_infantil,
                    t.quantidade_matricula_fundamental,
                    t.quantidade_matricula_medio,
                    t.quantidade_docente_educacao_basica,
                    COALESCE((
                      SELECT
                        SUM(
                          CASE
                            WHEN SAFE_CAST(num AS INT64) = 88888 THEN 0
                            ELSE SAFE_CAST(num AS INT64)
                          END
                        )
                      FROM UNNEST(
                            REGEXP_EXTRACT_ALL(
                              TO_JSON_STRING(t),
                              r'"quantidade_profissional_[^"]+":\\s*([0-9]+)'
                            )
                          ) AS num
                    ), 0) AS quantidade_profissional
                FROM `basedosdados.br_inep_censo_escolar.escola` AS t
                WHERE t.ano = {year_censo}
                  AND t.id_municipio IN ({muni_ids_sql})
                  AND t.regular = 1
                  AND t.tipo_situacao_funcionamento = '1'
            )

            SELECT
                d.id_escola,
                d.id_municipio,
                d.dependencia_administrativa,
                d.etapas_modalidades_oferecidas,
                d.endereco,
                d.latitude,
                d.longitude,
                c.rede,
                c.quantidade_matricula_infantil,
                c.quantidade_matricula_fundamental,
                c.quantidade_matricula_medio,
                c.quantidade_docente_educacao_basica,
                c.quantidade_profissional
            FROM dir AS d
            JOIN cen AS c USING (id_escola)
        """
    return bd.read_sql(query, billing_project_id=billing_project_id)