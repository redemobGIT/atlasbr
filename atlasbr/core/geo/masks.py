"""
AtlasBR - Core Geo Logic (Masks).
"""
import geopandas as gpd

def clip_to_mask(gdf: gpd.GeoDataFrame, mask: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Pure logic: clips geometries to a mask."""
    return gpd.overlay(gdf, mask, how="intersection")