"""
AtlasBR - Core Geo Spatial Logic (H3 & Interpolation).
"""
import h3
import geopandas as gpd
from shapely.geometry import Polygon
from typing import Tuple, List, Literal

try:
    from tobler.area_weighted import area_interpolate
    TOBLER_AVAILABLE = True
except ImportError:
    TOBLER_AVAILABLE = False

def generate_h3_grid(
    bbox: Tuple[float, float, float, float],
    resolution: int,
    crs: str
) -> gpd.GeoDataFrame:
    """Generates an empty H3 grid covering the bounding box."""
    minx, miny, maxx, maxy = bbox
    
    # Define polygon for H3 (requires Lat/Lon)
    geo_json_poly = {
        "type": "Polygon",
        "coordinates": [[
            [minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny]
        ]]
    }
    
    # Polyfill
    hex_ids = h3.polyfill(geo_json_poly, resolution, geo_json_conformant=True)
    
    # Create Geometries
    polys = []
    indices = []
    for hid in hex_ids:
        # h3 returns (lat, lon), shapely needs (lon, lat)
        boundary = h3.h3_to_geo_boundary(hid, geo_json=True)
        polys.append(Polygon(boundary))
        indices.append(hid)
        
    gdf = gpd.GeoDataFrame({"geometry": polys, "h3_index": indices}, crs="EPSG:4326")
    
    # Project to requested CRS
    return gdf.to_crs(crs).set_index("h3_index")

def interpolate_area_weighted(
    source_gdf: gpd.GeoDataFrame,
    target_gdf: gpd.GeoDataFrame,
    extensive_vars: List[str],
    intensive_vars: List[str]
) -> gpd.GeoDataFrame:
    """Wraps Tobler for areal interpolation."""
    if not TOBLER_AVAILABLE:
        raise ImportError("Tobler is required. Install via `pip install tobler`.")
        
    # Ensure CRS match
    if source_gdf.crs != target_gdf.crs:
        source_gdf = source_gdf.to_crs(target_gdf.crs)

    return area_interpolate(
        source_df=source_gdf,
        target_df=target_gdf,
        extensive_variables=extensive_vars,
        intensive_variables=intensive_vars,
        allocate_total=False
    )