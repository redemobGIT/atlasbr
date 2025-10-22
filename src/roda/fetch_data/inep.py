from typing import Sequence, Optional
import pandas as pd
import basedosdados as bd

from roda.utils.sql import load_query, sql_list


def fetch_schools(
    muni_ids: Sequence[int | str],
    *,
    year_censo: int = 2023,
    billing_project_id: str | None = None,
    query: Optional[str] = None,
) -> pd.DataFrame:
    """
    Busca dados do Censo Escolar para os municípios especificados.
    """
    muni_ids_sql = sql_list(muni_ids, quote=True, pad=7)

    if query is None:
        query = load_query(
            "fetch_schools.sql", muni_ids_sql=muni_ids_sql, year_censo=year_censo
        )
    else:
        query = query.format(muni_ids_sql=muni_ids_sql, year_censo=year_censo)

    return bd.read_sql(query, billing_project_id=billing_project_id)
