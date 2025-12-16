"""
AtlasBR - Infrastructure Geo Adapter (IBGE).

Handles downloading raw shapefiles from IBGE FTP servers.
Utilizes local disk caching to avoid redundant downloads of large shapefiles.
"""
import geopandas as gpd
from pathlib import Path
from functools import lru_cache

from atlasbr.settings import logger
from atlasbr.infra.storage.cache import (
    cached_download, 
    cached_extract_zip, 
    url_to_filename, 
    find_first_file
)

# Map requested year to the closest available IBGE 'Área Urbanizada' study
URL_URBAN_AREAS = {
    #2005: "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2005/areas_urbanizadas_do_Brasil_2005_shapes.zip",
    #2015: "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2015/Shape/AreasUrbanizadasDoBrasil_2015.zip",
    2019: "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2019/Shapefile/AreasUrbanizadas2019_Brasil.zip"
}

@lru_cache(maxsize=1)
def fetch_urban_area_raw_gdf(year: int) -> gpd.GeoDataFrame:
    """
    Downloads the full Brazil Urban Area shapefile for the closest epoch.
    Returns the raw GeoDataFrame (cached).
    """
    # Find closest available year
    epoch = min(URL_URBAN_AREAS.keys(), key=lambda k: abs(k - year))
    url = URL_URBAN_AREAS[epoch]
    
    logger.info(f"    ⬇️  Fetching Urban Areas (Epoch {epoch}) from IBGE (cached)...")

    # 1. Download Zip (Cached)
    rel_zip = Path("ibge") / "urban_areas" / url_to_filename(url, suffix=".zip")
    zip_path = cached_download(url, relpath=rel_zip, timeout=300) # Large file, longer timeout

    # 2. Extract (Cached)
    # We extract into a folder named after the zip hash to keep versions distinct
    extract_dir = zip_path.with_suffix("") 
    cached_extract_zip(zip_path, extract_dir=extract_dir)

    # 3. Find Shapefile
    # IBGE structure varies (sometimes nested folders), so we search recursively
    shp_path = find_first_file(extract_dir, "*.shp")
    
    if not shp_path:
        raise FileNotFoundError(f"No .shp file found in extracted Urban Areas for epoch {epoch}.")
    
    return gpd.read_file(shp_path)