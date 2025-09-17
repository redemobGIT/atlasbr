from __future__ import annotations
from typing import Iterable
import geopandas as gpd
from shapely.geometry import Polygon
import h3

def make_h3_grid(gdf: gpd.GeoDataFrame, res: int, *, ea_epsg: int) -> gpd.GeoDataFrame:
    study = gdf.to_crs(4326).unary_union
    geoms = list(study.geoms) if hasattr(study, "geoms") else [study]
    hex_ids: set[str] = set()
    for geom in geoms:
        if geom.is_empty:
            continue
        boundary = list(geom.exterior.coords)
        poly_latlon = [(y, x) for x, y in boundary]
        gj = {"type": "Polygon", "coordinates": [poly_latlon]}
        hex_ids.update(h3.polyfill(gj, res))
    polys = [Polygon([(lon, lat) for lat, lon in h3.h3_to_geo_boundary(h, True)]) for h in hex_ids]
    hexes = gpd.GeoDataFrame({"hex_id": list(hex_ids)}, geometry=polys, crs=4326)
    return hexes.to_crs(ea_epsg)

def validate_geoms(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = gdf.copy()
    out["geometry"] = out.geometry.buffer(0)
    return out[~out.geometry.is_empty]

def tobler_interpolate(src: gpd.GeoDataFrame, dst: gpd.GeoDataFrame, fields: Iterable[str], *, extensive: bool = True) -> gpd.GeoDataFrame:
    from tobler.area_weighted import area_interpolate
    out = dst.copy()
    result = area_interpolate(src, dst, extensive=fields if extensive else None, intensive=None if extensive else fields)
    out[fields] = result[fields]
    return out

def mass_conservation_check(src: gpd.GeoDataFrame, dst: gpd.GeoDataFrame, fields: Iterable[str]) -> dict[str, float]:
    return {f: float(abs(src[f].sum() - dst[f].sum())) for f in fields}
