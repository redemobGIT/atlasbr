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
        # ... (cole a query da função fetch_rais aqui) ...
    """
    return bd.read_sql(query, billing_project_id=billing_project_id)