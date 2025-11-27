"""
AtlasBR - Core Geo Operations (Clipping & Masking).
"""
import geopandas as gpd
import pandas as pd
from typing import Tuple
from atlasbr.core.geo.utils import to_local_utm, clean_geometries

def prepare_tracts(raw_tracts: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Standardizes raw tract data:
    1. Standardizes ID column (zfill 15).
    2. Projects to UTM.
    3. Cleans invalid geometries.
    """
    gdf = raw_tracts.copy()
    
    # Standardize ID
    if "code_tract" in gdf.columns:
        gdf["id_setor_censitario"] = gdf["code_tract"].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(15)
    
    # Project & Clean
    gdf = to_local_utm(gdf)
    gdf = clean_geometries(gdf)
    
    return gdf.set_index("id_setor_censitario")[["geometry"]]

def create_urban_mask(
    urban_gdf: gpd.GeoDataFrame,
    bbox: Tuple[float, float, float, float],
    target_crs: str
) -> gpd.GeoDataFrame:
    """
    Creates a single dissolved polygon mask from the raw national urban file.
    """
    # 1. Project to target CRS (usually UTM)
    if urban_gdf.crs != target_crs:
        urban_gdf = urban_gdf.to_crs(target_crs)

    # 2. Filter by Bounding Box (Spatial Index)
    minx, miny, maxx, maxy = bbox
    urban_slice = urban_gdf.cx[minx:maxx, miny:maxy]

    if urban_slice.empty:
        return gpd.GeoDataFrame({"geometry": []}, crs=target_crs)

    # 3. Dissolve and Buffer
    # buffer(500) adds a 500m margin to include peri-urban areas
    union_geom = urban_slice.unary_union
    buffered_geom = union_geom.buffer(500)

    return gpd.GeoDataFrame({"geometry": [buffered_geom]}, crs=target_crs)

def clip_to_mask(gdf: gpd.GeoDataFrame, mask: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Clips the input GDF to the mask polygon.
    """
    # Reset index is required because overlay drops the index
    gdf_reset = gdf.reset_index()
    
    clipped = gpd.overlay(gdf_reset, mask, how="intersection")
    
    # Restore Index (assuming id_setor_censitario exists)
    if "id_setor_censitario" in clipped.columns:
        clipped = clipped.set_index("id_setor_censitario")
        
    return clipped