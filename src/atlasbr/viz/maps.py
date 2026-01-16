from __future__ import annotations

import math
from typing import Optional, Tuple

import geopandas as gpd
import matplotlib.axes


def to_local_utm(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        raise ValueError("GeoDataFrame must have a CRS.")
    gdf_ll = gdf.to_crs(epsg=4326)
    lon = float(gdf_ll.geometry.x.mean())
    lat = float(gdf_ll.geometry.y.mean())
    epsg = _utm_epsg(lon, lat)
    return gdf_ll.to_crs(epsg=epsg)


def add_basemap(ax: matplotlib.axes.Axes, crs, source: str) -> None:
    try:
        import contextily as ctx
    except ImportError as exc:
        raise ImportError(
            "Basemaps require 'contextily'. Install via `pip install atlasbr[viz]`."
        ) from exc

    provider = _resolve_provider(ctx, source)
    ctx.add_basemap(ax, crs=crs, source=provider)


def add_north_arrow(ax: matplotlib.axes.Axes) -> None:
    x0, x1 = ax.get_xlim()
    y0, y1 = ax.get_ylim()
    dx = x1 - x0
    dy = y1 - y0

    x = x0 + 0.06 * dx
    y = y1 - 0.08 * dy
    ax.annotate(
        "N",
        xy=(x, y),
        xytext=(x, y - 0.06 * dy),
        arrowprops={"arrowstyle": "-|>", "lw": 1.2},
        ha="center",
        va="center",
        fontsize=11,
        fontweight="bold",
    )


def add_scale_bar(ax: matplotlib.axes.Axes) -> None:
    x0, x1 = ax.get_xlim()
    y0, y1 = ax.get_ylim()
    width = x1 - x0
    height = y1 - y0

    target = width * 0.22
    length = _nice_length(target)

    x = x0 + 0.06 * width
    y = y0 + 0.06 * height

    ax.plot([x, x + length], [y, y], lw=3, solid_capstyle="butt")
    ax.text(
        x + length / 2,
        y + 0.015 * height,
        _format_length(length),
        ha="center",
        va="bottom",
        fontsize=9,
    )


def _utm_epsg(lon: float, lat: float) -> int:
    zone = int((lon + 180.0) // 6.0) + 1
    return 32600 + zone if lat >= 0 else 32700 + zone


def _nice_length(meters: float) -> float:
    if meters <= 0:
        return 0.0
    exp = math.floor(math.log10(meters))
    base = meters / (10**exp)
    for m in (1, 2, 5, 10):
        if base <= m:
            return float(m * (10**exp))
    return float(10 * (10**exp))


def _format_length(meters: float) -> str:
    if meters >= 1000:
        km = meters / 1000.0
        if km.is_integer():
            return f"{int(km)} km"
        return f"{km:.1f} km"
    return f"{int(meters):,} m"


def _resolve_provider(ctx, source: str):
    if not hasattr(ctx, "providers"):
        return None
    parts = source.split(".")
    node = ctx.providers
    for p in parts:
        if not hasattr(node, p):
            raise ValueError(f"Unknown basemap source: {source!r}")
        node = getattr(node, p)
    return node
