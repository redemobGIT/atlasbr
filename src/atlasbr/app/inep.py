"""
AtlasBR - Application Layer for Schools.
"""
import pandas as pd
import geopandas as gpd
from typing import List, Union, Optional

from atlasbr.core.catalog.inep import get_schools_spec
from atlasbr.core.logic import geocoding
from atlasbr.infra.geo import resolver
from atlasbr.settings import logger, resolve_billing_id
from atlasbr.core.types import PlaceInput

def load_schools(
    places: List[PlaceInput],
    *,
    year: int = 2023,
    gcp_billing: Optional[str] = None,
    as_gdf: bool = True,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Loads School locations and metrics.
    """
    # 1. Configuration
    project_id = resolve_billing_id(gcp_billing)

    # 2. Resolve Inputs
    muni_ids = resolver.resolve_places_to_ids(places)
    
    # 3. Get Spec
    spec = get_schools_spec(year)
    
    # 4. Fetch Data
    # Local import to prevent eager loading of basedosdados
    from atlasbr.infra.adapters import inep_bd
    df_schools = inep_bd.fetch_schools_from_bd(
        munis=muni_ids,
        year=year,
        billing_id=project_id
    )
    
    # 5. Convert to GeoDataFrame
    if as_gdf:
        logger.info(f"    🌍 Converting {len(df_schools)} schools to geometry...")
        gdf_schools = geocoding.points_from_coords(
            df_schools, 
            lat_col="latitude", 
            lon_col="longitude"
        )
        logger.info(f"✅ Loaded {len(gdf_schools)} schools.")
        return gdf_schools
    
    logger.info(f"✅ Loaded {len(df_schools)} schools (Tabular).")
    return df_schools