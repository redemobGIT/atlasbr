"""
AtlasBR - Core Geo Utilities.
"""
import geopandas as gpd

def to_local_utm(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reprojects a GeoDataFrame to the appropriate local UTM zone (SIRGAS 2000)."""
    if gdf.empty or gdf.geometry.isnull().all():
         return gdf
    
    # estimate_utm_crs finds the best EPSG code based on the geometry centroid
    utm_crs = gdf.estimate_utm_crs(datum_name="SIRGAS 2000")
    return gdf.to_crs(utm_crs)

def clean_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Fixes invalid geometries using buffer(0).
    Only applies fix to geometries marked as invalid to save time.
    """
    if gdf.empty:
        return gdf

    # Identify invalid geometries
    invalid_mask = ~gdf.is_valid
    
    if invalid_mask.any():
        # Apply buffer(0) only to invalid rows
        # We copy to avoid SettingWithCopy warnings if slice
        gdf.loc[invalid_mask, "geometry"] = gdf.loc[invalid_mask, "geometry"].buffer(0)
    
    return gdf