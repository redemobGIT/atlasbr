"""
AtlasBR - Core Logic for Geocoding.

Reusable logic to attach geometries to dataframes based on common keys (CEP).
"""

import pandas as pd
import geopandas as gpd
from shapely import wkt

def points_from_coords(
    df: pd.DataFrame, 
    lat_col: str = "latitude", 
    lon_col: str = "longitude",
    crs: str = "EPSG:4326"
) -> gpd.GeoDataFrame:
    """Converts a DataFrame with lat/lon columns into a GeoDataFrame."""
    return gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs=crs
    )

def geocode_by_cep(
    data_df: pd.DataFrame,
    cep_df: pd.DataFrame,
    data_cep_col: str = "cep",
    cep_ref_col: str = "cep",
    geometry_col: str = "centroide"
) -> gpd.GeoDataFrame:
    """
    Merges a dataset with a CEP reference table and converts to GeoDataFrame.
    """
    # 1. Prepare Keys
    data_df = data_df.copy()
    data_df["_merge_key"] = data_df[data_cep_col].astype(str).str.zfill(8)
    
    cep_ref = cep_df.copy()
    cep_ref[cep_ref_col] = cep_ref[cep_ref_col].astype(str).str.zfill(8)

    # 2. Merge
    # Use suffixes to ensure we know which 'cep' is which if names collide
    merged = pd.merge(
        data_df,
        cep_ref[[cep_ref_col, geometry_col]],
        left_on="_merge_key",
        right_on=cep_ref_col,
        how="left",
        suffixes=("", "_ref") 
    )
    
    # 3. Parse Geometry (WKT -> Shapely)
    # Filter out missing geometries to avoid parser errors
    mask_geom = merged[geometry_col].notna()
    
    # Create a temporary geometry series
    geoms = pd.Series(index=merged.index, dtype="object")
    geoms[mask_geom] = merged.loc[mask_geom, geometry_col].apply(wkt.loads)
    
    # 4. Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(merged, geometry=geoms, crs="EPSG:4326")
    
    # 5. Clean up auxiliary columns safely
    # If the ref column name collided, it might be named 'cep_ref' now due to suffixes logic above
    cols_to_drop = ["_merge_key", geometry_col]
    
    # Check if the ref column name changed due to collision
    if cep_ref_col in gdf.columns:
        cols_to_drop.append(cep_ref_col)
    elif f"{cep_ref_col}_ref" in gdf.columns:
        cols_to_drop.append(f"{cep_ref_col}_ref")
        
    gdf = gdf.drop(columns=cols_to_drop, errors="ignore")
    
    return gdf