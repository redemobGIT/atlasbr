"""
AtlasBR - Core Geo Spatial Logic (H3 & Interpolation).

Provides robust H3 grid generation (adapting to library versions) and
areal interpolation wrappers.
"""
import h3
import pandas as pd
import geopandas as gpd
import shapely
from shapely.geometry import Polygon
from packaging.version import Version
from typing import List, Union, Tuple, Optional # <--- Added Tuple here
from warnings import warn

# Optional Tobler import for robust areal interpolation
try:
    from tobler.area_weighted import area_interpolate
    TOBLER_AVAILABLE = True
except ImportError:
    TOBLER_AVAILABLE = False

# Check GeoPandas version for union operations
GPD_10 = Version(gpd.__version__) >= Version("1.0.0dev")

def circumradius(resolution: int) -> float:
    """
    Calculates the circumradius of an H3 hexagon at a given resolution (in meters).
    """
    if Version(h3.__version__) >= Version("4.0.0"):
        return h3.average_hexagon_edge_length(resolution, unit="m")
    return h3.edge_length(resolution, unit="m")

def _swap_coords(coords: List[List[float]]) -> Tuple[float, float]:
    """Swaps (Lon, Lat) to (Lat, Lon) for H3 compatibility."""
    return tuple((p[1], p[0]) for p in coords)

def _to_hex(source, resolution: int = 6, return_geoms: bool = True) -> Union[pd.Series, gpd.GeoDataFrame]:
    """
    Internal helper: Generates hex grid for a single Polygon geometry.
    Handles compatibility between H3 v3 (polyfill) and v4 (polygon_to_cells).
    """
    # 1. Generate Hex IDs
    try:
        # --- H3 v4 Logic (Requires H3Shape object) ---
        if isinstance(source, Polygon):
            # Shapely gives (Lon, Lat), H3 wants (Lat, Lon)
            if hasattr(source.exterior, "coords"):
                outer = _swap_coords(list(source.exterior.coords))
                holes = [_swap_coords(list(h.coords)) for h in source.interiors]
                
                # Use the class available in H3 v4
                poly_h3 = h3.LatLngPoly(outer, *holes)
                hex_ids = h3.polygon_to_cells(poly_h3, resolution)
            else:
                # Fallback for unexpected geometry types
                raise AttributeError("Geometry does not have coords")
        else:
             # Fallback logic if not a simple Polygon
             raise AttributeError("Not a Polygon")

    except (AttributeError, NameError, TypeError):
        # --- H3 v3 Logic (Accepts GeoJSON dict) ---
        if hasattr(source, "__geo_interface__"):
            poly_geojson = source.__geo_interface__
        else:
            poly_geojson = shapely.geometry.mapping(source)
            
        hex_ids = h3.polyfill(poly_geojson, resolution, geo_json_conformant=True)

    if not return_geoms:
        return pd.Series(list(hex_ids))

    # 2. Generate Geometries
    polys = []
    # Detect available boundary function
    to_boundary = getattr(h3, "cell_to_boundary", getattr(h3, "h3_to_geo_boundary", None))
    
    if not to_boundary:
        raise ImportError("Could not find H3 boundary function. Check 'h3' library installation.")

    for hid in hex_ids:
        # h3 returns (lat, lon), shapely needs (lon, lat)
        boundary = to_boundary(hid, geo_json=True)
        polys.append(Polygon(boundary))

    return gpd.GeoDataFrame(
        {"h3_index": list(hex_ids)}, 
        geometry=polys, 
        crs="EPSG:4326"
    ).set_index("h3_index")

def h3fy(
    source: gpd.GeoDataFrame, 
    resolution: int = 6, 
    clip: bool = False, 
    buffer: bool = False, 
    return_geoms: bool = True
) -> Union[pd.Series, gpd.GeoDataFrame]:
    """
    Generate a hexgrid GeoDataFrame that fully covers the geometry of a source GeoDataFrame.
    """
    if source.crs is None:
        raise ValueError("Source GeoDataFrame must have a valid CRS.")

    orig_crs = source.crs
    clipper = source

    # 1. Project to Lat/Lon (EPSG:4326) required for H3
    if source.crs.is_geographic:
        if buffer:
            # Estimate UTM to buffer in meters, then project back
            utm_crs = source.estimate_utm_crs()
            dist = circumradius(resolution)
            source = source.to_crs(utm_crs).buffer(dist).to_crs(4326)
        else:
            source = source.to_crs(4326)
    else:
        # Source is projected
        if buffer:
            dist = circumradius(resolution)
            source = source.to_crs(4326) # Simplify: project first
        else:
            source = source.to_crs(4326)

    # 2. Merge geometries into a single footprint
    if GPD_10:
        source_unary = shapely.force_2d(source.union_all())
    else:
        source_unary = shapely.force_2d(source.unary_union)

    # 3. Fill footprint with Hexagons
    if isinstance(source_unary, Polygon):
        hexagons = _to_hex(source_unary, resolution, return_geoms)
    else:
        # Handle MultiPolygon by iterating parts
        output = []
        for geom in source_unary.geoms:
            hexes = _to_hex(geom, resolution, return_geoms)
            output.append(hexes)
        hexagons = pd.concat(output)

    # 4. Post-processing (Clip & Reproject)
    if return_geoms and clip:
        clipper_4326 = clipper.to_crs(4326)
        hexagons = gpd.clip(hexagons, clipper_4326)

    if return_geoms and not hexagons.crs.equals(orig_crs):
        hexagons = hexagons.to_crs(orig_crs)

    return hexagons

def interpolate_area_weighted(
    source_gdf: gpd.GeoDataFrame,
    target_gdf: gpd.GeoDataFrame,
    extensive_vars: List[str],
    intensive_vars: List[str]
) -> gpd.GeoDataFrame:
    """
    Transfers attributes from Source (Tracts) to Target (H3) using areal weighting.
    Requires 'tobler'.
    """
    if not TOBLER_AVAILABLE:
        raise ImportError("Tobler is required for interpolation. Install via `pip install tobler`.")
        
    # Ensure CRS match for accurate area calculation
    if source_gdf.crs != target_gdf.crs:
        source_gdf = source_gdf.to_crs(target_gdf.crs)

    return area_interpolate(
        source_df=source_gdf,
        target_df=target_gdf,
        extensive_variables=extensive_vars,
        intensive_variables=intensive_vars,
        allocate_total=False 
    )