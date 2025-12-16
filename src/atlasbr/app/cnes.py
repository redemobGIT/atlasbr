"""
AtlasBR - Application Layer for CNES.
"""
import pandas as pd
import geopandas as gpd
from typing import List, Union, Optional

from atlasbr.core.catalog.cnes import get_cnes_spec
from atlasbr.core.logic import geocoding
from atlasbr.infra.geo import resolver
from atlasbr.settings import logger, resolve_billing_id
from atlasbr.core.types import PlaceInput

def load_cnes(
    places: List[PlaceInput],
    *,
    year: int = 2023,
    month: int = 9,
    gcp_billing: Optional[str] = None,
    geocode: bool = False,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Loads Healthcare establishments (CNES) with infrastructure metrics.
    """
    # 1. Configuration
    project_id = resolve_billing_id(gcp_billing)

    # 2. Resolve Inputs
    muni_ids = resolver.resolve_places_to_ids(places)
    
    # 3. Get Spec
    spec = get_cnes_spec(year, month)
    
    # 4. Fetch Data
    # Local import for lazy loading (prevents eager basedosdados load)
    from atlasbr.infra.adapters import cnes_bd
    df_cnes = cnes_bd.fetch_cnes_from_bd(
        munis=muni_ids,
        year=year,
        month=month,
        billing_id=project_id
    )
    
    # 5. Optional: Geocoding
    if geocode:
        from atlasbr.infra.adapters import ceps_bd
        df_ceps = ceps_bd.fetch_ceps_from_bd(
            munis=muni_ids,
            billing_id=project_id
        )
        
        logger.info(f"    🌍 Geocoding {len(df_cnes)} healthcare units via CEP...")
        gdf_cnes = geocoding.geocode_by_cep(
            data_df=df_cnes,
            cep_df=df_ceps,
            data_cep_col="cep"
        )
        logger.info(f"✅ Loaded {len(gdf_cnes)} CNES units (Geolocated).")
        return gdf_cnes
    
    logger.info(f"✅ Loaded {len(df_cnes)} CNES units (Tabular).")
    return df_cnes