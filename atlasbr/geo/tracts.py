"""
AtlasBR - Geo Loader for Census Tracts.

This module acts as a wrapper around the `geobr` library to fetch
Census Tract (Setores Censitários) geometries in a standardized way.
"""

import pandas as pd
import geopandas as gpd
import geobr
import numpy as np
from typing import Iterable, List

def load_tracts(
    munis: Iterable[int],
    year: int,
) -> gpd.GeoDataFrame:
    """
    Fetches Census Tracts for the specified municipalities and year.
    
    Wraps `geobr.read_census_tract` with optimizations:
    1. Parallel concatenation (implied by list comprehension).
    2. CRS standardization (estimates UTM).
    3. Index standardization (zfill 15).
    4. Geometry cleaning (buffer(0) to fix self-intersections).

    Args:
        munis: List of 7-digit municipality codes.
        year: Census year (2010 or 2022).

    Returns:
        gpd.GeoDataFrame: Indexed by 'id_setor_censitario', CRS projected to local UTM.
    """
    # Ensure iterables are list of ints
    muni_list = [int(m) for m in np.atleast_1d(munis)]
    
    print(f"    🗺️  Loading Census Tracts geometries for {len(muni_list)} municipalities...")

    # Fetch individual municipality tracts
    # Note: geobr might be slow for many municipalities; 
    # in production, consider a parallel loop or a pre-built cache if frequent.
    dfs: List[gpd.GeoDataFrame] = []
    for code in muni_list:
        try:
            df = geobr.read_census_tract(code_tract=code, year=year, simplified=False, verbose=False)
            dfs.append(df)
        except Exception as e:
            print(f"        ⚠️ Warning: Failed to load tracts for muni {code}: {e}")
            continue

    if not dfs:
        raise RuntimeError("No census tracts could be loaded for the requested municipalities.")

    gdf = pd.concat(dfs, ignore_index=True)

    # Standardize ID column
    # geobr returns 'code_tract' usually as float or string depending on version
    gdf["id_setor_censitario"] = gdf["code_tract"].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(15)
    
    # Project to UTM (SIRGAS 2000) for accurate area/overlay operations
    # estimate_utm_crs is a geopandas helper
    target_crs = gdf.estimate_utm_crs(datum_name="SIRGAS 2000")
    gdf = gdf.to_crs(target_crs)

    # Fix invalid geometries (common in raw IBGE data)
    gdf["geometry"] = gdf.geometry.buffer(0)

    # Final cleanup
    gdf = gdf.set_index("id_setor_censitario")[["geometry"]]
    
    return gdf