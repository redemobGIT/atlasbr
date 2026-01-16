"""
AtlasBR - Infrastructure Adapter for CNES (Base dos Dados).
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from atlasbr.core.catalog.cnes import (
    CNES_INFRASTRUCTURE_GROUPS,
    CNES_UNIT_CODES,
    CnesThemeSpec,
)
from atlasbr.settings import get_billing_id, logger


def fetch_cnes_from_bd(
    spec: CnesThemeSpec,
    *,
    munis: Iterable[int],
    billing_id: str | None = None,
) -> pd.DataFrame:
    try:
        import basedosdados as bd
    except ImportError as exc:
        raise ImportError(
            "The 'bd' strategy for CNES requires 'basedosdados'. "
            "Install via `pip install atlasbr[bd]`."
        ) from exc

    project_id = billing_id or get_billing_id()
    munis_sql = _munis_in_sql(munis)
    units_sql = _units_in_sql()
    infra_sql = _build_infra_selects()

    query = (
        "WITH estab AS (\n"
        "    SELECT\n"
        "        e.id_estabelecimento_cnes,\n"
        "        e.id_municipio,\n"
        "        e.cep,\n"
        "        e.tipo_unidade,\n"
        "        e.tipo_pessoa,\n"
        "        e.indicador_vinculo_sus,\n"
        "        e.indicador_atencao_hospitalar,\n"
        "        CASE\n"
        "          WHEN REGEXP_CONTAINS(\n"
        "            TO_JSON_STRING(e),\n"
        "            r'\"indicador_gestao_alta_[^\"]+\":\\s*1'\n"
        "          ) THEN 'alta'\n"
        "          WHEN REGEXP_CONTAINS(\n"
        "            TO_JSON_STRING(e),\n"
        "            r'\"indicador_gestao_media_[^\"]+\":\\s*1'\n"
        "          ) THEN 'media'\n"
        "          WHEN REGEXP_CONTAINS(\n"
        "            TO_JSON_STRING(e),\n"
        "            r'\"indicador_gestao_basica_[^\"]+\":\\s*1'\n"
        "          ) THEN 'basica'\n"
        "        END AS complexidade,\n"
        f"        {infra_sql}\n"
        f"    FROM `{spec.table_estab}` AS e\n"
        f"    WHERE e.id_municipio IN ({munis_sql})\n"
        f"      AND e.ano = {int(spec.year)}\n"
        f"      AND e.mes = {int(spec.month)}\n"
        f"      AND e.tipo_unidade IN ({units_sql})\n"
        "      AND e.tipo_pessoa = '3'\n"
        "),\n"
        "workers AS (\n"
        "    SELECT\n"
        "        t.id_estabelecimento_cnes,\n"
        "        COALESCE(SUM(\n"
        "            CASE\n"
        "              WHEN SAFE_CAST(num AS INT64) = 88888 THEN 0\n"
        "              ELSE SAFE_CAST(num AS INT64)\n"
        "            END\n"
        "        ), 0) AS quantidade_trabalhadores_saude\n"
        f"    FROM `{spec.table_prof}` AS t\n"
        "    LEFT JOIN UNNEST(\n"
        "      REGEXP_EXTRACT_ALL(\n"
        "        TO_JSON_STRING(t),\n"
        "        r'\"quantidade_profissional_[^\"]+\":\\s*([0-9]+)'\n"
        "      )\n"
        "    ) AS num ON TRUE\n"
        f"    WHERE t.id_municipio IN ({munis_sql})\n"
        f"      AND t.ano = {int(spec.year)}\n"
        f"      AND t.mes = {int(spec.month)}\n"
        "    GROUP BY t.id_estabelecimento_cnes\n"
        ")\n"
        "SELECT\n"
        "    e.*,\n"
        "    COALESCE(w.quantidade_trabalhadores_saude, 0)\n"
        "        AS quantidade_trabalhadores_saude\n"
        "FROM estab AS e\n"
        "LEFT JOIN workers AS w USING (id_estabelecimento_cnes)"
    )

    logger.info(f"    🏥 Fetching CNES {spec.month}/{spec.year} from Base dos Dados...")
    return bd.read_sql(query, billing_project_id=project_id)


def _build_infra_selects() -> str:
    selects: list[str] = []
    for alias, cols in CNES_INFRASTRUCTURE_GROUPS.items():
        expr = " + ".join(f"COALESCE({c}, 0)" for c in cols)
        selects.append(f"{expr} AS {alias}")
    return ",\n        ".join(selects)


def _munis_in_sql(munis: Iterable[int]) -> str:
    values = [f"'{int(m):07d}'" for m in munis]
    if not values:
        raise ValueError("munis must contain at least one municipality code.")
    return ", ".join(values)


def _units_in_sql() -> str:
    values = [f"'{c}'" for c in CNES_UNIT_CODES.keys()]
    if not values:
        raise ValueError("CNES_UNIT_CODES must not be empty.")
    return ", ".join(values)
