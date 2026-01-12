"""
AtlasBR - Infrastructure Adapter for RAIS (Base dos Dados).
"""

from __future__ import annotations

from typing import Iterable, Sequence

import pandas as pd

from atlasbr.core.catalog.rais import RaisThemeSpec
from atlasbr.settings import get_billing_id, logger


def fetch_rais_from_bd(
    spec: RaisThemeSpec,
    *,
    munis: Iterable[int],
    year: int,
    billing_id: str | None = None,
) -> pd.DataFrame:
    try:
        import basedosdados as bd
    except ImportError as exc:
        raise ImportError(
            "The 'bd_table' strategy for RAIS requires 'basedosdados'. "
            "Install via `pip install atlasbr[bd]`."
        ) from exc

    project_id = billing_id or get_billing_id()
    munis_sql = _munis_in_sql(munis)
    cols_sql = _cols_sql(spec.required_columns)

    query = (
        f"SELECT {cols_sql}\n"
        f"FROM `{spec.table_id}`\n"
        f"WHERE ano = {int(year)}\n"
        f"  AND id_municipio IN ({munis_sql})"
    )

    logger.info(f"    🏭 Fetching RAIS {year} from Base dos Dados...")
    return bd.read_sql(query, billing_project_id=project_id)


def _munis_in_sql(munis: Iterable[int]) -> str:
    values = [f"'{int(m):07d}'" for m in munis]
    if not values:
        raise ValueError("munis must contain at least one municipality code.")
    return ", ".join(values)


def _cols_sql(columns: Sequence[str]) -> str:
    if not columns:
        raise ValueError("spec.required_columns must not be empty.")
    return ", ".join(columns)
