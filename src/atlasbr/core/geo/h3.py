"""
AtlasBR - Core Geo Spatial Logic (H3 & Interpolation).

Provides robust H3 grid generation (adapting to library versions) and
areal interpolation wrappers using Tobler.
"""
import pandas as pd
import geopandas as gpd
import shapely
from shapely.geometry import Polygon
from packaging.version import Version
from typing import List, Union, Tuple, Optional, Any
from warnings import warn

from atlasbr.settings import logger

# Check GeoPandas version for union operations
GPD_10 = Version(gpd.__version__) >= Version("1.0.0dev")

def _require_h3() -> Any:
    """Lazy loader for H3."""
    try:
        import h3
        return h3
    except ImportError:
        raise ImportError(
            "The 'h3' library is required for this operation. "
            "Please install it via `pip install atlasbr[geo]`."
        )

def _require_tobler() -> Any:
    """Lazy loader for Tobler."""
    try:
        from tobler.area_weighted import area_interpolate
        return area_interpolate
    except ImportError:
        raise ImportError(
            "The 'tobler' library is required for areal interpolation. "
            "Please install it via `pip install atlasbr[geo]`."
        )

def circumradius(resolution: int) -> float:
    """
    Calculates the circumradius (center-to-vertex distance) of an H3 hexagon 
    at a given resolution (in meters).
    
    Note: For a regular hexagon, the circumradius is equal to the edge length.
    H3 hexagons are not perfectly regular, but edge length is the standard proxy
    for coverage buffering calculations.
    """
    h3 = _require_h3()
    if Version(h3.__version__) >= Version("4.0.0"):
        return h3.average_hexagon_edge_length(resolution, unit="m")
    return h3.edge_length(resolution, unit="m")

def _swap_coords(coords: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    """Swap (x, y) -> (y, x) for all coordinate pairs."""
    return [(y, x) for (x, y) in coords]

def _to_hex(
    source, 
    resolution: int = 6, 
    return_geoms: bool = True
) -> Union[pd.Series, gpd.GeoDataFrame]:
    """
    Internal helper: Generates hex grid for a single Polygon geometry.
    Handles compatibility between H3 v3 (polyfill) and v4 (polygon_to_cells).
    """
    h3 = _require_h3()
    
    # 1. Generate Hex IDs
    try:
        # --- H3 v4 Logic (Requires H3Shape object) ---
        if isinstance(source, Polygon) and hasattr(source.exterior, "coords"):
            # Shapely gives (Lon, Lat), H3 v4 wants (Lat, Lon)
            outer = _swap_coords(list(source.exterior.coords))
            holes = [_swap_coords(list(h.coords)) for h in source.interiors]
            
            # Use the class available in H3 v4
            poly_h3 = h3.LatLngPoly(outer, *holes)
            hex_ids = h3.polygon_to_cells(poly_h3, resolution)
        else:
            # Fallback for complex shapes if logic above fails
            raise AttributeError("Not a simple Polygon")

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
    has_cell_to_boundary = hasattr(h3, "cell_to_boundary")
    
    for hid in hex_ids:
        if has_cell_to_boundary:
            # H3 v4: cell_to_boundary returns ((lat, lon), ...) tuple
            # We must manually swap to (lon, lat) for Shapely
            boundary_latlng = h3.cell_to_boundary(hid)
            boundary = [(lng, lat) for (lat, lng) in boundary_latlng]
        else:
            # H3 v3: h3_to_geo_boundary(..., geo_json=True) returns (lon, lat)
            boundary = h3.h3_to_geo_boundary(hid, geo_json=True)
            
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

    # Only load H3 if we actually need to calculate it
    _require_h3()

    orig_crs = source.crs
    clipper = source.copy()

    # 1. Project to Lat/Lon (EPSG:4326) required for H3
    if source.crs.is_geographic:
        if buffer:
            warn(
                "Source GeoDataFrame is in a geographic CRS. "
                "Estimating UTM zone to perform metric buffering.",
                stacklevel=2
            )
            utm_crs = source.estimate_utm_crs()
            dist = circumradius(resolution)
            source = source.to_crs(utm_crs).buffer(dist).to_crs(4326)
        else:
            source = source.to_crs(4326)
    else:
        # Source is projected
        if buffer:
            # Check units to ensure metric buffering works
            crs_info = source.crs.to_dict() if hasattr(source.crs, "to_dict") else {}
            try:
                crs_units = str(crs_info.get("units", "")).lower()
                # If units missing (e.g. from EPSG code lookup), try axis info
                if not crs_units and hasattr(source.crs, "axis_info"):
                     crs_units = source.crs.axis_info[0].unit_name.lower()
            except (AttributeError, IndexError):
                crs_units = "unknown"

            if crs_units not in ("m", "metre", "meter", "us-ft", "ft-us"):
                warn(f"Unknown CRS units '{crs_units}'. Assuming meters for buffering.")
            
            dist = circumradius(resolution)
            if crs_units in ("us-ft", "ft-us"):
                dist *= 3.281

            # Buffer in projected CRS, then project to 4326
            source = source.buffer(dist).to_crs(4326)
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
    extensive_vars: Optional[List[str]] = None,
    intensive_vars: Optional[List[str]] = None,
    preserve_totals: bool = True
) -> gpd.GeoDataFrame:
    """
    Transfers attributes from Source (Tracts) to Target (H3) using areal weighting.
    Wrapper around `tobler.area_weighted.area_interpolate`.

    Args:
        source_gdf: Source geometries (e.g. Tracts).
        target_gdf: Target geometries (e.g. Hexagons).
        extensive_vars: Variables to sum (e.g. population, households).
        intensive_vars: Variables to average (e.g. income, density).
        preserve_totals: If True, ensures the sum of extensive vars is preserved (allocate_total=True).
    """
    area_interpolate = _require_tobler()

    if extensive_vars is None: extensive_vars = []
    if intensive_vars is None: intensive_vars = []
        
    # Ensure CRS match for accurate area calculation
    if not source_gdf.crs.equals(target_gdf.crs):
        logger.info("    ⚠️ Reprojecting source to match target CRS for interpolation...")
        source_gdf = source_gdf.to_crs(target_gdf.crs)

    return area_interpolate(
        source_df=source_gdf,
        target_df=target_gdf,
        extensive_variables=extensive_vars,
        intensive_variables=intensive_vars,
        allocate_total=preserve_totals 
    )