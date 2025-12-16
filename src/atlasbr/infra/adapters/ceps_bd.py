"""
AtlasBR - Infrastructure Adapter for CEPs (Base dos Dados).
"""

import pandas as pd
from typing import Iterable

from atlasbr.settings import get_billing_id

def fetch_ceps_from_bd(
    munis: Iterable[int],
    billing_id: str | None = None,
) -> pd.DataFrame:
    """
    Fetches CEP coordinates for the given municipalities.
    Returns raw DataFrame with 'cep' and 'centroide' (WKT).
    """
    try:
        import basedosdados as bd
    except ImportError as e:
        raise ImportError(
            "Geocoding via CEPs requires the optional dependency 'basedosdados'. "
            "Please install it via `pip install atlasbr[bd]`."
        ) from e

    project_id = billing_id or get_billing_id()

    muni_list_sql = ", ".join(f"'{int(m):07d}'" for m in munis)
    
    # We only need the CEP code and the geometry (centroid)
    query = f"""
        SELECT cep, centroide
        FROM `basedosdados.br_bd_diretorios_brasil.cep`
        WHERE id_municipio IN ({muni_list_sql})
    """
    
    print(f"    📍 Fetching CEP coordinates from Base dos Dados...")
    df = bd.read_sql(query, billing_project_id=project_id)
    
    # Standardize CEP to 8 digits string just in case
    if not df.empty:
        df["cep"] = df["cep"].astype(str).str.zfill(8)
        
    return df