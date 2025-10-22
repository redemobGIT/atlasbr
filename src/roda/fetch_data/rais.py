from typing import Sequence, Iterable, Optional
import pandas as pd
import basedosdados as bd

from roda.utils.sql import sql_list, load_query
from roda.utils.cnae import CNAE_PROBLEM_REGEX


def fetch_rais(
    muni_ids: Sequence[int | str],
    years: Sequence[int] | int = 2022,
    *,
    billing_project_id: str | None = None,
    query: Optional[str] = None,
) -> pd.DataFrame:
    """
    Extrai dados da RAIS com limite adaptativo para CNAEs problemáticos.
    """
    mun_sql = sql_list(muni_ids, quote=True, pad=7)
    year_seq = years if isinstance(years, Iterable) else [years]
    yr_sql = sql_list(year_seq)

    _RX_PROBLEM = CNAE_PROBLEM_REGEX

    if query is None:
        query = load_query(
            "fetch_rais.sql",
            mun_sql=mun_sql,
            yr_sql=yr_sql,
            _RX_PROBLEM=_RX_PROBLEM,
        )
    else:
        query = query.format(mun_sql=mun_sql, yr_sql=yr_sql, _RX_PROBLEM=_RX_PROBLEM)

    return bd.read_sql(query, billing_project_id=billing_project_id)
