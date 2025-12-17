"""
AtlasBR - Infrastructure Adapter for Census (Base dos Dados).

Handles fetching Census data via BigQuery SQL.
"""
import pandas as pd
from typing import List, Optional, Set

from atlasbr.core.catalog.census import CensusThemeSpec
from atlasbr.settings import get_billing_id, logger


def fetch_census_bd(
    spec: CensusThemeSpec,
    munis: List[int],
    billing_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetches Census data from Base dos Dados (BigQuery).

    Args:
        spec: The Census theme specification.
        munis: List of 7-digit municipality IDs.
        billing_id: Google Cloud Project ID for billing.

    Returns:
        pd.DataFrame: Data indexed by 'id_setor_censitario'.
    """
    try:
        import basedosdados as bd
    except ImportError as e:
        raise ImportError(
            "Fetching from Base dos Dados requires the 'basedosdados' lib. "
            "Install via `pip install atlasbr[bd]`."
        ) from e

    project_id = billing_id or get_billing_id()

    # 1. Construct SQL
    # Always include the tract identifier so downstream joins work reliably.
    raw_cols = ["id_setor_censitario", *spec.required_columns]

    # De-duplicate while preserving order
    seen: Set[str] = set()
    cols = [c for c in raw_cols if not (c in seen or seen.add(c))]
    columns_str = ", ".join(cols)

    # Format municipalities for SQL IN clause
    muni_list_sql = ", ".join(f"'{int(m):07d}'" for m in munis)

    query = f"""
        SELECT {columns_str}
        FROM `{spec.table_id}`
        WHERE id_municipio IN ({muni_list_sql})
    """

    logger.info(
        f"    ☁️  Querying Base dos Dados ({spec.theme} {spec.year})..."
    )

    # 2. Execute
    df = bd.read_sql(query, billing_project_id=project_id)

    # 3. Post-processing
    # Standardize column names so BD and FTP strategies return compatible outputs
    if getattr(spec, "column_map", None):
        df = df.rename(columns=spec.column_map)

    # Standardize ID and set index for joins
    if "id_setor_censitario" in df.columns:
        df["id_setor_censitario"] = (
            df["id_setor_censitario"].astype(str).str.zfill(15)
        )
        df = df.set_index("id_setor_censitario")

    # Ensure numeric types for data columns
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")

    return df