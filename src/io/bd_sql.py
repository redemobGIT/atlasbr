from __future__ import annotations
import pandas as pd

def load_bd_sql(query: str, *, project: str | None = None) -> pd.DataFrame:
    """Executa SQL no BigQuery (Base dos Dados) e retorna DataFrame."""
    try:
        import pandas_gbq as gbq  # type: ignore
    except Exception as exc:
        raise RuntimeError("pandas-gbq não instalado. Use `pip install pandas-gbq`.") from exc
    return gbq.read_gbq(query, project_id=project)
