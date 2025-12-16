"""
AtlasBR - Core Geo Operations (Clipping & Masking).
"""
import geopandas as gpd
import pandas as pd
from typing import Tuple, Union, Any
from atlasbr.core.geo.utils import to_local_utm, clean_geometries
from atlasbr.settings import logger

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
        # Removes .0 suffix if present (common in int->float->str conversions)
        gdf["id_setor_censitario"] = (
            gdf["code_tract"]
            .astype(str)
            .str.replace(r'\.0$', '', regex=True)
            .str.zfill(15)
        )
    
    # Project & Clean
    gdf = to_local_utm(gdf)
    gdf = clean_geometries(gdf)
    
    # Ensure index is set if the column exists
    if "id_setor_censitario" in gdf.columns:
        return gdf.set_index("id_setor_censitario")[["geometry"]]
    
    return gdf[["geometry"]]

def create_urban_mask(
    urban_gdf: gpd.GeoDataFrame,
    bbox: Tuple[float, float, float, float],
    target_crs: Any  # Changed from Union[str, gpd.tools.osr...] to Any
) -> gpd.GeoDataFrame:
    """
    Creates a single dissolved polygon mask from the raw national urban file.
    """
    # 1. Project to target CRS (usually UTM) if needed
    if not urban_gdf.crs.equals(target_crs):
        urban_gdf = urban_gdf.to_crs(target_crs)

    # 2. Filter by Bounding Box (Spatial Index)
    minx, miny, maxx, maxy = bbox
    urban_slice = urban_gdf.cx[minx:maxx, miny:maxy]

    if urban_slice.empty:
        logger.warning("    ⚠️ Urban mask slice is empty. Returning empty mask.")
        return gpd.GeoDataFrame({"geometry": []}, crs=target_crs)

    # 3. Dissolve and Buffer
    union_geom = urban_slice.unary_union
    
    if union_geom.is_empty:
         return gpd.GeoDataFrame({"geometry": []}, crs=target_crs)

    buffered_geom = union_geom.buffer(500)

    return gpd.GeoDataFrame({"geometry": [buffered_geom]}, crs=target_crs)

def clip_to_mask(gdf: gpd.GeoDataFrame, mask: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Clips the input GDF to the mask polygon.
    """
    if mask.empty:
        return gdf.iloc[0:0] 

    if not mask.crs.equals(gdf.crs):
        mask = mask.to_crs(gdf.crs)

    return gpd.clip(gdf, mask, keep_geom_type=True)