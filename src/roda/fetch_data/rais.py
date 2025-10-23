from typing import Sequence, Iterable, Optional, Any
import pandas as pd
import basedosdados as bd

from roda.defaults.base import FETCH_RAIS_SQL_PATH
from roda.utils.sql import sql_list, format_query_from_file, format_query_from_string
from roda.utils.cnae import CNAE_PROBLEM_REGEX


def fetch_rais(
    muni_ids: Sequence[int | str],
    years: Sequence[int] | int = 2022,
    *,
    billing_project_id: str | None = None,
    query_filepath: Optional[str] = None,
    query_string: Optional[str] = None,
    **params: Optional[Any],
) -> pd.DataFrame:
    """
    Extrai dados da RAIS com limite adaptativo para CNAEs problemáticos.
    """
    if query_filepath and query_string:
        raise ValueError(
            "Os parâmetros 'query_filepath' e 'query_string' são mutuamente "
            "exclusivos. Forneça apenas um."
        )

    final_query = ""
    try:
        if query_filepath:
            final_query = format_query_from_file(query_filepath, **params)

        elif query_string:
            final_query = format_query_from_string(query_string, **params)

        else:
            mun_sql = sql_list(muni_ids, quote=True, pad=7)
            year_seq = years if isinstance(years, Iterable) else [years]
            yr_sql = sql_list(year_seq)
            _RX_PROBLEM = CNAE_PROBLEM_REGEX

            final_query = format_query_from_file(
                FETCH_RAIS_SQL_PATH,
                mun_sql=mun_sql,
                yr_sql=yr_sql,
                _RX_PROBLEM=_RX_PROBLEM,
            )

    except (FileNotFoundError, ValueError, IOError) as e:
        raise RuntimeError(f"Erro ao preparar a query da RAIS: {e}")

    return bd.read_sql(final_query, billing_project_id=billing_project_id)
