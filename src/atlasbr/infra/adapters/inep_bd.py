"""
AtlasBR - Infrastructure Adapter for Schools (Base dos Dados).
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from atlasbr.core.catalog.inep import SchoolsThemeSpec
from atlasbr.settings import get_billing_id, logger


def fetch_schools_from_bd(
    spec: SchoolsThemeSpec,
    *,
    munis: Iterable[int],
    year: int,
    billing_id: str | None = None,
) -> pd.DataFrame:
    try:
        import basedosdados as bd
    except ImportError as exc:
        raise ImportError(
            "The 'bd' strategy for INEP Schools requires "
            "'basedosdados'. Install via `pip install atlasbr[bd]`."
        ) from exc

    project_id = billing_id or get_billing_id()
    munis_sql = _munis_in_sql(munis)

    query = (
        "WITH dir AS (\n"
        "    SELECT\n"
        "        id_escola,\n"
        "        id_municipio,\n"
        "        dependencia_administrativa,\n"
        "        etapas_modalidades_oferecidas,\n"
        "        endereco,\n"
        "        latitude,\n"
        "        longitude\n"
        f"    FROM `{spec.table_directory}`\n"
        f"    WHERE id_municipio IN ({munis_sql})\n"
        "      AND latitude IS NOT NULL\n"
        "      AND longitude IS NOT NULL\n"
        "),\n"
        "cen AS (\n"
        "    SELECT\n"
        "        t.id_escola,\n"
        "        CASE\n"
        "          WHEN CAST(t.rede AS STRING) IN ('1', '2', '3')\n"
        "          THEN 'Publica'\n"
        "          ELSE 'Privada'\n"
        "        END AS rede,\n"
        "        t.quantidade_matricula_infantil,\n"
        "        t.quantidade_matricula_fundamental,\n"
        "        t.quantidade_matricula_medio,\n"
        "        t.quantidade_docente_educacao_basica,\n"
        "        COALESCE((\n"
        "          SELECT SUM(\n"
        "              CASE\n"
        "                WHEN SAFE_CAST(num AS INT64) = 88888 THEN 0\n"
        "                ELSE SAFE_CAST(num AS INT64)\n"
        "              END\n"
        "          )\n"
        "          FROM UNNEST(\n"
        "            REGEXP_EXTRACT_ALL(\n"
        "              TO_JSON_STRING(t),\n"
        "              r'\"quantidade_profissional_[^\"]+\":\\s*([0-9]+)'\n"
        "            )\n"
        "          ) AS num\n"
        "        ), 0) AS quantidade_profissional\n"
        f"    FROM `{spec.table_census}` AS t\n"
        f"    WHERE t.ano = {int(year)}\n"
        f"      AND t.id_municipio IN ({munis_sql})\n"
        "      AND t.regular = 1\n"
        "      AND t.tipo_situacao_funcionamento = '1'\n"
        ")\n"
        "SELECT\n"
        "    d.id_escola,\n"
        "    d.id_municipio,\n"
        "    d.dependencia_administrativa,\n"
        "    d.etapas_modalidades_oferecidas,\n"
        "    d.endereco,\n"
        "    d.latitude,\n"
        "    d.longitude,\n"
        "    c.rede,\n"
        "    c.quantidade_matricula_infantil,\n"
        "    c.quantidade_matricula_fundamental,\n"
        "    c.quantidade_matricula_medio,\n"
        "    c.quantidade_docente_educacao_basica,\n"
        "    c.quantidade_profissional\n"
        "FROM dir AS d\n"
        "JOIN cen AS c USING (id_escola)"
    )

    logger.info(f"    🎓 Fetching Schools {year} from Base dos Dados...")
    return bd.read_sql(query, billing_project_id=project_id)


def _munis_in_sql(munis: Iterable[int]) -> str:
    values = [f"'{int(m):07d}'" for m in munis]
    if not values:
        raise ValueError("munis must contain at least one municipality code.")
    return ", ".join(values)
