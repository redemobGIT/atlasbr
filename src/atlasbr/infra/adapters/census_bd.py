"""
AtlasBR - Infrastructure Adapter for Base dos Dados (BigQuery).

This module handles the physical connection to BigQuery using the `basedosdados` library.
It constructs and executes SQL queries based on the specs provided by the Core layer.
"""

import pandas as pd
from typing import List, Iterable

from atlasbr.settings import get_billing_id

def fetch_from_bd(
    table_id: str,
    columns: List[str],
    munis: Iterable[int],
    billing_id: str | None = None,
) -> pd.DataFrame:
    """
    Fetches raw columns from a Base dos Dados table for specific municipalities.

    Args:
        table_id: The full BigQuery table ID (e.g., 'basedosdados.br_ibge...').
        columns: List of column names to select.
        munis: List of 7-digit IBGE municipality codes.
        billing_id: Google Cloud Project ID for billing.

    Returns:
        pd.DataFrame: DataFrame indexed by 'id_setor_censitario'.
    """
    try:
        import basedosdados as bd
    except ImportError as e:
        raise ImportError(
            "The 'bd_table' strategy requires the optional dependency 'basedosdados'. "
            "Please install it via `pip install atlasbr[bd]` or use a different strategy."
        ) from e

    project_id = billing_id or get_billing_id()

    # Defensive formatting: ensure munis are strings of length 7
    muni_list_sql = ", ".join(f"'{int(m):07d}'" for m in munis)

    # Construct the SELECT clause
    cols_sql = ", ".join(columns)

    query = f"""
        SELECT id_setor_censitario, {cols_sql}
        FROM `{table_id}`
        WHERE SUBSTR(id_setor_censitario, 1, 7) IN ({muni_list_sql})
    """

    print(f"    ☁️  Fetching {len(columns)} columns from {table_id}...")

    df = bd.read_sql(query, billing_project_id=project_id)

    # Standardize index immediately
    return df.set_index("id_setor_censitario")