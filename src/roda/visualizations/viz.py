from typing import List, Dict, Tuple, Optional, Any

import numpy as np
import pandas as pd
import geopandas as gpd
import mapclassify as mc

import plotly.express.colors as colors
import plotly.graph_objects as go

from roda.utils.geo import prepare_geodata
from .utils import visibility_mask, labels_from_bins
from .styles import SLIDER_STYLE, DROPDOWN_STYLE, COLORBAR_STYLE, build_coloraxis

def _calculate_variable_specs(
    gdf: gpd.GeoDataFrame,
    vars_to_show: List[str],
    id_col: str,
    year_col: str,
    locs: List[str],
    years: List[int],
    clip_q: Optional[Tuple[float, float]],
    log_color: bool,
    scheme: Optional[str],
    k: int,
    scheme_kwargs: Optional[Dict],
    colorscale: str,
) -> Tuple[Dict[str, Dict[int, Dict[str, np.ndarray]]],
           Dict[str, Dict[int, Dict[str, Any]]]]:
    """
    Pre-compute (per variable, per year):
      - the plotted z-array (continuous or class indices)
      - the raw values (for hover)
      - the color-axis spec (continuous min/max OR discrete bins/labels/colorscale)
    """
    var_specs: Dict[str, Dict[int, Dict[str, Any]]] = {v: {} for v in vars_to_show}
    data_cache: Dict[str, Dict[int, Dict[str, np.ndarray]]] = {v: {} for v in vars_to_show}

    for var in vars_to_show:
        # id x year table for this variable, ordered to match "locs"
        pivot_df = gdf.pivot(index=id_col, columns=year_col, values=var)
        pivot_df.columns = pivot_df.columns.astype(int)
        pivot_df = pivot_df.reindex(locs)

        for yr in years:
            s_raw = pd.to_numeric(pivot_df.get(yr), errors="coerce")

            # Per-(var,year) clipping like many GIS defaults
            if clip_q:
                lo, hi = s_raw.quantile(list(clip_q))
                s_clip = s_raw.clip(lo, hi)
            else:
                s_clip = s_raw.copy()

            # Optional transform applied per-(var,year)
            if log_color:
                z_cont = np.log10(np.clip(s_clip.to_numpy(dtype=float), 1e-12, None))
            else:
                z_cont = s_clip.to_numpy(dtype=float)

            finite_mask = np.isfinite(z_cont)
            finite_vals = z_cont[finite_mask]

            # Build the spec for this (var,year)
            spec: Dict[str, Any] = {"log_color": log_color, "title": var}

            if scheme and finite_vals.size >= 2 and (mc is not None):
                # Defensive: reduce k if too few distinct values
                nunique = np.unique(finite_vals).size
                k_eff = max(1, min(k, nunique))
                if k_eff == 1:
                    # Fall back to continuous with tiny span
                    cmin = float(np.nanmin(finite_vals)) if finite_vals.size else 0.0
                    cmax = cmin + 1e-9
                    data_cache[var][yr] = {"raw": s_raw.to_numpy(), "z": z_cont}
                    spec.update({"type": "continuous", "cmin": cmin, "cmax": cmax, "colorscale": colorscale})
                else:
                    klass = mc.classify(finite_vals, scheme=scheme, k=k_eff, **(scheme_kwargs or {}))
                    bins = np.asarray(klass.bins, dtype=float)  # length = k_eff
                    # Digitize the *whole* array (NaNs remain NaN)
                    z_idx = np.full_like(z_cont, fill_value=np.nan, dtype=float)
                    z_idx[finite_mask] = np.digitize(finite_vals, bins=bins, right=True).astype(float)
                    labels = labels_from_bins(bins)
                    positions = np.linspace(0, 1, k_eff)
                    disc_colors = colors.sample_colorscale(colorscale, positions)
                    spec.update({
                        "type": "discrete",
                        "k": k_eff,
                        "labels": labels,
                        "colorscale": [[i / max(1, (k_eff - 1)), c] for i, c in enumerate(disc_colors)],
                    })
                    data_cache[var][yr] = {"raw": s_raw.to_numpy(), "z": z_idx}
            else:
                # Continuous per-(var,year) (closest to GeoPandas .explore default)
                if finite_vals.size == 0:
                    cmin, cmax = 0.0, 1.0
                else:
                    cmin = float(np.nanmin(finite_vals))
                    cmax = float(np.nanmax(finite_vals))
                    if not np.isfinite(cmin): cmin = 0.0
                    if not np.isfinite(cmax): cmax = cmin + 1.0
                    if cmin == cmax:
                        cmax = cmin + 1e-9  # avoid zero-span color scales
                spec.update({"type": "continuous", "cmin": cmin, "cmax": cmax, "colorscale": colorscale})
                data_cache[var][yr] = {"raw": s_raw.to_numpy(), "z": z_cont}

            var_specs[var][yr] = spec

    return data_cache, var_specs

def hexmap_slider_dropdown(
    gdf: gpd.GeoDataFrame,
    *,
    year_col: str = "year",
    id_col: str = "hex_id",
    vars_to_show: List[str] = ("habitantes",),
    map_height: int = 700,
    tiles: str = "open-street-map",
    colorscale: str = "RdYlBu",
    opacity: float = 0.7,
    clip_q: Optional[Tuple[float, float]] = None,   # ← set to None to match .explore behavior
    log_color: bool = False,
    mapbox_zoom: Optional[float] = None,
    title_prefix: str = "Mapa | ",
    scheme: Optional[str] = None,
    k: int = 5,
    scheme_kwargs: Optional[Dict] = None,
) -> go.Figure:
    """
    Creates an interactive choropleth map with a year slider and a
    variable dropdown (one legend/colorscheme per (variable × year)).
    """
    # 1) Data and per-(var,year) specs
    gdf_proc, geojson, locs, years = prepare_geodata(gdf, id_col, year_col, list(vars_to_show))
    data_cache, var_specs = _calculate_variable_specs(
        gdf_proc, list(vars_to_show), id_col, year_col, locs, years,
        clip_q, log_color, scheme, k, scheme_kwargs, colorscale,
    )

    # 2) Figure + one coloraxis per (var,year)
    fig = go.Figure()
    num_vars = len(vars_to_show)
    num_years = len(years)

    axis_name = {}
    axis_counter = 1
    for i, var in enumerate(vars_to_show):
        for j, yr in enumerate(years):
            name = f"coloraxis{axis_counter}"
            axis_name[(i, j)] = name
            fig.layout[name] = build_coloraxis(var_specs[var][yr])
            axis_counter += 1

    # 3) All traces (one per (var,year)), attach their own coloraxis
    for i, var in enumerate(vars_to_show):
        for j, yr in enumerate(years):
            is_first = (i == 0 and j == 0)
            fig.add_trace(
                go.Choroplethmap(               # If your Plotly version lacks this, use go.Choroplethmapbox + mapbox_* keys.
                    locations=locs,
                    z=data_cache[var][yr]["z"],
                    customdata=data_cache[var][yr]["raw"],
                    geojson=geojson,
                    featureidkey=f"properties.{id_col}",
                    coloraxis=axis_name[(i, j)],
                    marker_line_width=0,
                    visible=is_first,
                    hovertemplate=(
                        f"%{{location}}<br>{var} ({yr}): "
                        "%{customdata:,.0f}<extra></extra>"
                    ),
                )
            )

    # 4) Build one slider *per variable*, each showing that variable’s traces only
    sliders_by_var = {}
    for i, var in enumerate(vars_to_show):
        steps = []
        for j, yr in enumerate(years):
            vis = visibility_mask(num_vars, num_years, i, j)
            steps.append(dict(
                label=str(yr),
                method="update",
                args=[
                    {"visible": vis},
                    {"title": f"{title_prefix}{var} — {yr}"}
                ],
            ))
        sliders_by_var[var] = [dict(**SLIDER_STYLE, active=0, steps=steps)]

    # 5) Dropdown that (i) shows var at its first year and (ii) swaps in the right slider
    dropdown_buttons = []
    for i, var in enumerate(vars_to_show):
        vis = visibility_mask(num_vars, num_years, i, 0)
        dropdown_buttons.append(dict(
            label=var,
            method="update",
            args=[
                {"visible": vis},                                  # data update
                {"sliders": sliders_by_var[var],                   # layout update: swap slider
                 "title": f"{title_prefix}{var} — {years[0]}"}     # title
            ],
        ))

    # 6) Final layout
    xmin, ymin, xmax, ymax = gdf_proc.total_bounds
    fig.update_layout(
        height=map_height,
        title=f"{title_prefix}{vars_to_show[0]} — {years[0]}",
        map_style=tiles,               # new 'map' keys; for mapbox use mapbox_style, mapbox_center, mapbox_zoom
        map_center={"lon": (xmin + xmax) / 2, "lat": (ymin + ymax) / 2},
        map_zoom=mapbox_zoom if mapbox_zoom is not None else 10,
        updatemenus=[dict(**DROPDOWN_STYLE, active=0, buttons=dropdown_buttons)],
        sliders=sliders_by_var[vars_to_show[0]],
        margin=dict(l=0, r=0, t=80, b=0),
        showlegend=False,
    )
    fig.update_traces(marker_opacity=opacity)
    return fig
