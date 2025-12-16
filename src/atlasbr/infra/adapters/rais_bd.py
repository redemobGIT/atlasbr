"""
AtlasBR - Infrastructure Adapter for RAIS (Base dos Dados).
"""

import pandas as pd
from typing import List, Iterable
from atlasbr.settings import get_billing_id

def fetch_rais_from_bd(
    table_id: str,
    columns: List[str],
    munis: Iterable[int],
    year: int,
    billing_id: str | None = None,
) -> pd.DataFrame:
    """
    Fetches raw RAIS data. No statistical processing here.
    """
    try:
        import basedosdados as bd
    except ImportError as e:
        raise ImportError(
            "The 'bd_table' strategy for RAIS requires the optional dependency 'basedosdados'. "
            "Please install it via `pip install atlasbr[bd]`."
        ) from e

    project_id = billing_id or get_billing_id()
    muni_list_sql = ", ".join(f"'{int(m):07d}'" for m in munis)
    cols_sql = ", ".join(columns)

    query = f"""
        SELECT {cols_sql}
        FROM `{table_id}`
        WHERE ano = {year}
          AND id_municipio IN ({muni_list_sql})
    """
    
    print(f"    🏭 Fetching RAIS {year} from Base dos Dados...")
    return bd.read_sql(query, billing_project_id=project_id)