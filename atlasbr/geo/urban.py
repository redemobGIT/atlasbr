"""
AtlasBR - Urban Area Handling.

Manages the download and processing of 'Áreas Urbanizadas' from IBGE.
Used to clip Census Tracts to the actual urban footprint.
"""

import requests
import zipfile
import tempfile
import geopandas as gpd
from pathlib import Path
from io import BytesIO
from functools import lru_cache
from typing import Tuple

# --- Constants ---

# Map requested year to the closest available IBGE 'Área Urbanizada' study
URL_URBAN_AREAS = {
    2005: "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2005/areas_urbanizadas_do_Brasil_2005_shapes.zip",
    2015: "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2015/Shape/AreasUrbanizadasDoBrasil_2015.zip",
    2019: "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2019/Shapefile/AreasUrbanizadas2019_Brasil.zip"
}

# --- Core Functions ---

@lru_cache(maxsize=1)
def _fetch_urban_area_gdf(epoch: int) -> gpd.GeoDataFrame:
    """
    Downloads and caches the full Brazil Urban Area shapefile for a specific epoch.
    Cached in memory to avoid re-downloading during the same session.
    """
    url = URL_URBAN_AREAS[epoch]
    print(f"    🏙️  Downloading Urban Areas (Epoch {epoch})...")

    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to download Urban Areas from {url}") from e

    with zipfile.ZipFile(BytesIO(response.content)) as zf:
        with tempfile.TemporaryDirectory() as tmpdir:
            zf.extractall(tmpdir)
            
            # Find the .shp file
            shapefile_path = next(Path(tmpdir).rglob("*.shp"), None)
            if not shapefile_path:
                raise FileNotFoundError("No .shp file found in Urban Area zip.")
            
            gdf = gpd.read_file(shapefile_path)
            return gdf

def get_urban_mask(
    year: int,
    target_crs: str,
    bbox: Tuple[float, float, float, float]
) -> gpd.GeoDataFrame:
    """
    Creates a unified urban polygon mask for the given bounding box.

    Args:
        year: The reference year (snaps to closest available urban study).
        target_crs: The CRS to project the mask into (must match tracts).
        bbox: (minx, miny, maxx, maxy) in the Target CRS. Used to filter the huge national file.

    Returns:
        gpd.GeoDataFrame: A single row GDF containing the dissolved urban geometry.
    """
    # 1. Select closest epoch (2005, 2015, 2019)
    epoch = min(URL_URBAN_AREAS.keys(), key=lambda k: abs(k - year))
    
    # 2. Get the National Data (Cached)
    national_urban = _fetch_urban_area_gdf(epoch)

    # 3. Project and Filter
    # Note: cx uses the index coordinates. We need to ensure national_urban is in target_crs
    # BEFORE clipping, or transform the BBOX to the national_urban CRS.
    # Usually safer to transform the big file's CRS only for the slice we need, 
    # but since we don't know the national CRS reliably without reading, 
    # we rely on geopandas handling the CRS transform if we set it.
    
    # For efficiency: Transform BBOX to 4326 (likely raw format) might be faster, 
    # but let's stick to correctness:
    if national_urban.crs != target_crs:
        # Optimization: Only transform valid geometries
        local_urban = national_urban.to_crs(target_crs)
    else:
        local_urban = national_urban

    # 4. Spatial Index Filter (Clip to BBox)
    minx, miny, maxx, maxy = bbox
    urban_slice = local_urban.cx[minx:maxx, miny:maxy]

    if urban_slice.empty:
        # Return empty GDF with correct CRS
        return gpd.GeoDataFrame({"geometry": []}, crs=target_crs)

    # 5. Dissolve and Buffer
    # Buffer(0) fixes topology; Buffer(500) creates the "Metro Area" effect defined in your logic
    # unary_union dissolves all polygons into one MultiPolygon
    union_geom = urban_slice.unary_union
    
    # We buffer slightly (e.g., 500m) to include peri-urban areas as requested
    buffered_geom = union_geom.buffer(500)

    return gpd.GeoDataFrame({"geometry": [buffered_geom]}, crs=target_crs)