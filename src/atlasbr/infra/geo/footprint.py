"""
AtlasBR - Infrastructure Geo Adapter (IBGE).

Handles downloading raw shapefiles from IBGE FTP servers.
"""
import requests
import zipfile
import tempfile
import geopandas as gpd
from pathlib import Path
from io import BytesIO
from functools import lru_cache
from atlasbr.settings import logger

# Map requested year to the closest available IBGE 'Área Urbanizada' study
URL_URBAN_AREAS = {
    # HACK: use 2019 only, correct smell later
    100: "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2005/areas_urbanizadas_do_Brasil_2005_shapes.zip",
    100: "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2015/Shape/AreasUrbanizadasDoBrasil_2015.zip",
    2019: "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2019/Shapefile/AreasUrbanizadas2019_Brasil.zip"
}

@lru_cache(maxsize=1)
def fetch_urban_area_raw_gdf(year: int) -> gpd.GeoDataFrame:
    """
    Downloads the full Brazil Urban Area shapefile for the closest epoch.
    Returns the raw GeoDataFrame (cached).
    """
    epoch = min(URL_URBAN_AREAS.keys(), key=lambda k: abs(k - year))
    url = URL_URBAN_AREAS[epoch]
    
    logger.info(f"Downloading Urban Areas (Epoch {epoch}) from IBGE...")

    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to download Urban Areas from {url}") from e

    with zipfile.ZipFile(BytesIO(response.content)) as zf:
        with tempfile.TemporaryDirectory() as tmpdir:
            zf.extractall(tmpdir)
            
            shapefile_path = next(Path(tmpdir).rglob("*.shp"), None)
            if not shapefile_path:
                raise FileNotFoundError("No .shp file found in Urban Area zip.")
            
            return gpd.read_file(shapefile_path)