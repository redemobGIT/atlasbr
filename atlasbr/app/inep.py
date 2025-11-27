"""
AtlasBR - Application Layer for Schools.
"""

import pandas as pd
import geopandas as gpd
from typing import List, Union, Tuple

from atlasbr.core.catalog.schools import get_schools_spec
from atlasbr.infra.adapters import schools_bd
from atlasbr.core.logic import geocoding
from atlasbr.geo import resolver

PlaceInput = Union[int, str, Tuple[str, str]]

def load_schools(
    places: List[PlaceInput],
    *,
    year: int = 2023,
    gcp_billing: str,
    as_gdf: bool = True,  # Default to True since schools usually imply location
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Loads School locations and metrics.
    
    Args:
        as_gdf: If True, returns a GeoDataFrame (Points). If False, returns raw DataFrame with lat/lon cols.
    """
    
    # 1. Resolve Inputs
    muni_ids = resolver.resolve_places(places)
    
    # 2. Get Spec
    spec = get_schools_spec(year)
    
    # 3. Fetch Data
    df_schools = schools_bd.fetch_schools_from_bd(
        munis=muni_ids,
        year=year,
        billing_id=gcp_billing
    )
    
    # 4. Convert to GeoDataFrame
    if as_gdf:
        print(f"    🌍 Converting {len(df_schools)} schools to geometry...")
        gdf_schools = geocoding.points_from_coords(
            df_schools, 
            lat_col="latitude", 
            lon_col="longitude"
        )
        print(f"✅ Loaded {len(gdf_schools)} schools.")
        return gdf_schools
    
    print(f"✅ Loaded {len(df_schools)} schools (Tabular).")
    return df_schools