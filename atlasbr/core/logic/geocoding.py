"""
AtlasBR - Core Logic for Geocoding.

Reusable logic to attach geometries to dataframes based on common keys (CEP).
"""

import pandas as pd
import geopandas as gpd
from shapely import wkt

def geocode_by_cep(
    data_df: pd.DataFrame,
    cep_df: pd.DataFrame,
    data_cep_col: str = "cep",
    cep_ref_col: str = "cep",
    geometry_col: str = "centroide"
) -> gpd.GeoDataFrame:
    """
    Merges a dataset with a CEP reference table and converts to GeoDataFrame.
    
    Args:
        data_df: The main data (e.g., RAIS jobs).
        cep_df: The reference data (CEPs + WKT geometry).
        data_cep_col: Column name in data_df containing CEPs.
        cep_ref_col: Column name in cep_df containing CEPs.
        geometry_col: Column name in cep_df containing WKT string.
        
    Returns:
        gpd.GeoDataFrame: The input data with geometry attached. 
                          Rows with invalid/missing CEPs will have empty geometry 
                          or be dropped depending on join type (Left join used here).
    """
    # 1. Ensure keys are strings and padded
    data_df = data_df.copy()
    data_df["_merge_key"] = data_df[data_cep_col].astype(str).str.zfill(8)
    
    # 2. Merge
    # Left join: we keep RAIS jobs even if we can't find the CEP location
    merged = pd.merge(
        data_df,
        cep_df[[cep_ref_col, geometry_col]],
        left_on="_merge_key",
        right_on=cep_ref_col,
        how="left"
    )
    
    # 3. Parse Geometry (WKT -> Shapely)
    # Filter out missing geometries to avoid parser errors
    mask_geom = merged[geometry_col].notna()
    
    # Create a temporary geometry series
    geoms = pd.Series(index=merged.index, dtype="object")
    geoms[mask_geom] = merged.loc[mask_geom, geometry_col].apply(wkt.loads)
    
    # 4. Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(merged, geometry=geoms, crs="EPSG:4326")
    
    # Clean up auxiliary columns
    gdf = gdf.drop(columns=["_merge_key", cep_ref_col, geometry_col])
    
    return gdf


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