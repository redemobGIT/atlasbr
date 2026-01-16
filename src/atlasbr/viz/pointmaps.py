from __future__ import annotations

from math import ceil
from typing import Optional, Tuple

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from atlasbr.viz.maps import add_basemap, add_north_arrow, add_scale_bar
from atlasbr.viz.maps import to_local_utm
from atlasbr.viz.types import MapStyle
from atlasbr.viz.validators import coerce_numeric, require_columns
from atlasbr.viz.validators import top_k_categories


def plot_point_heatmap(
    gdf: gpd.GeoDataFrame,
    *,
    value: str,
    hue: Optional[str] = None,
    hue_mode: str = "facet",
    top_k: int = 6,
    gridsize: int = 120,
    agg: str = "sum",
    style: Optional[MapStyle] = None,
) -> Tuple:
    style = style or MapStyle()
    cols = [value] + ([hue] if hue else [])
    require_columns(gdf, [c for c in cols if c])

    gdf_utm = to_local_utm(gdf)
    w = coerce_numeric(gdf_utm[value], fillna=0.0)

    if hue is None:
        fig, ax = plt.subplots()
        _hexbin(ax, gdf_utm, w, gridsize=gridsize, agg=agg)
        _decorate_map(ax, gdf_utm, style)
        return fig, ax

    if hue_mode not in {"facet", "topk_facet"}:
        raise ValueError(f"Unknown hue_mode: {hue_mode!r}")

    mapped, cats = top_k_categories(gdf_utm[hue], top_k=top_k)
    gdf2 = gdf_utm.copy()
    gdf2[hue] = mapped

    n = len(cats)
    ncols = 2 if n > 1 else 1
    nrows = int(ceil(n / ncols))

    fig, axes = plt.subplots(nrows=nrows, ncols=ncols)
    axes = np.atleast_1d(axes).ravel()

    for ax, cat in zip(axes, cats):
        sub = gdf2[gdf2[hue] == cat]
        ws = coerce_numeric(sub[value], fillna=0.0)
        _hexbin(ax, sub, ws, gridsize=gridsize, agg=agg)
        s2 = MapStyle(
            title=f"{style.title or ''}{' — ' if style.title else ''}{cat}",
            basemap=style.basemap,
            basemap_source=style.basemap_source,
            north_arrow=style.north_arrow,
            scale_bar=style.scale_bar,
            axis_off=style.axis_off,
        )
        _decorate_map(ax, sub, s2)

    for ax in axes[len(cats) :]:
        ax.set_visible(False)

    if fig is not None:
        fig.tight_layout()

    return fig, axes


def _hexbin(ax, gdf, weights, *, gridsize: int, agg: str) -> None:
    x = gdf.geometry.x.to_numpy()
    y = gdf.geometry.y.to_numpy()

    if agg == "count":
        c = None
        reduce_fn = None
    elif agg == "sum":
        c = weights.to_numpy()
        reduce_fn = np.sum
    elif agg == "mean":
        c = weights.to_numpy()
        reduce_fn = np.mean
    else:
        raise ValueError(f"Unknown agg: {agg!r}")

    hb = ax.hexbin(
        x,
        y,
        C=c,
        reduce_C_function=reduce_fn,
        gridsize=int(gridsize),
        mincnt=1,
        bins="log",
    )
    cb = plt.colorbar(hb, ax=ax)
    cb.set_label(_colorbar_label(agg, weights.name))


def _decorate_map(ax, gdf_utm, style: MapStyle) -> None:
    if style.basemap:
        add_basemap(ax, gdf_utm.crs, style.basemap_source)
    if style.north_arrow:
        add_north_arrow(ax)
    if style.scale_bar:
        add_scale_bar(ax)
    if style.axis_off:
        ax.set_axis_off()
    if style.title:
        ax.set_title(style.title)


def _colorbar_label(agg: str, value: Optional[str]) -> str:
    if agg == "count":
        return "Count (log10-binned)"
    if value is None:
        return f"{agg.capitalize()} (log10-binned)"
    return f"{agg.capitalize()} of {value} (log10-binned)"
