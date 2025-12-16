"""
AtlasBR - Infrastructure Adapter for Schools (Base dos Dados).
"""

import pandas as pd
import basedosdados as bd
from typing import Iterable, List
from atlasbr.settings import get_billing_id

def fetch_schools_from_bd(
    munis: Iterable[int],
    year: int,
    billing_id: str | None = None,
) -> pd.DataFrame:
    """
    Fetches school locations and metrics (enrollment, staff).
    Joins the 'Directory' table (coords) with 'Census' table (data).
    """
    project_id = billing_id or settings.get_billing_id()

    muni_list_sql = ", ".join(f"'{int(m):07d}'" for m in munis)
    
    # Identify tables
    table_dir = "basedosdados.br_bd_diretorios_brasil.escola"
    table_census = "basedosdados.br_inep_censo_escolar.escola"

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
            FROM `{table_dir}`
            WHERE id_municipio IN ({muni_list_sql})
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
        ),

        cen AS (
            SELECT
                t.id_escola,
                -- Classification Logic
                CASE
                  WHEN CAST(t.rede AS STRING) IN ('1','2','3') THEN 'Publica'
                  ELSE 'Privada'
                END AS rede,
                
                -- Metrics
                t.quantidade_matricula_infantil,
                t.quantidade_matricula_fundamental,
                t.quantidade_matricula_medio,
                t.quantidade_docente_educacao_basica,
                
                -- Dynamic Worker Sum (Regex over JSON)
                COALESCE((
                  SELECT SUM(
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
                
            FROM `{table_census}` AS t
            WHERE t.ano = {year}
              AND t.id_municipio IN ({muni_list_sql})
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
    
    print(f"    🎓 Fetching Schools {year} from Base dos Dados...")
    return bd.read_sql(query, billing_project_id=project_id)