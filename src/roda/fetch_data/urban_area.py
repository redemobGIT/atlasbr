import zipfile
import tempfile
import requests
from functools import lru_cache
from io import BytesIO
from pathlib import Path
import geopandas as gpd
from .ibge import URL_URBAN_AREAS

@lru_cache(maxsize=None)
def get_urban_area_gdf(year: int) -> gpd.GeoDataFrame:
    """Retrieves a GeoDataFrame of Brazilian urbanized areas for a given year.

    This function downloads official data from IBGE's "Área Urbanizada"
    (Urbanized Area) project. It automatically selects the dataset from the
    closest available epoch (2005, 2015, or 2019) to the specified `year`.

    The process involves streaming the ZIP archive into memory, extracting its
    contents to a temporary directory, and reading the first discovered
    shapefile (`.shp`) into a GeoDataFrame.

    Note:
        The `@lru_cache` decorator caches the result of this function. Subsequent
        calls with the same `year` will return the cached GeoDataFrame instantly
        without re-downloading the data.

    Args:
        year (int): The target year for the data. The function will use the
            data from the epoch nearest to this value.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame where each polygon represents an
            urbanized area in Brazil for the selected epoch.

    Raises:
        FileNotFoundError: If no `.shp` file is found within the downloaded
            ZIP archive.
        requests.exceptions.RequestException: If a network-related error
            occurs during the download.

    Example:
        >>> # Requesting data for 2016 will fetch the nearest available epoch (2015).
        >>> urban_areas_gdf = get_urban_area_gdf(2016)
        >>> print(f"CRS: {urban_areas_gdf.crs}")
        >>> print(urban_areas_gdf.head())
    """
    # Find the epoch (available year) closest to the requested year.
    epoch = min(URL_URBAN_AREAS, key=lambda k: abs(k - year))

    # Select the URL corresponding to the determined epoch.
    url = URL_URBAN_AREAS[epoch]
    print(f"🛰️  Selected epoch {epoch} for year {year}. Downloading from: {url.split('/')[-1]}")

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        raise

    with zipfile.ZipFile(BytesIO(response.content)) as zf:
        with tempfile.TemporaryDirectory() as tmpdir:
            zf.extractall(tmpdir)
            
            # Recursively find the first file with a .shp extension.
            shapefile_path = next(Path(tmpdir).rglob("*.shp"), None)
            
            if shapefile_path is None:
                raise FileNotFoundError("No .shp file found inside the downloaded archive.")
            
            return gpd.read_file(shapefile_path)