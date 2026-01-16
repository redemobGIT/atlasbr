"""
AtlasBR - Visualization Helpers.

Contains utility functions for preparing data, generating labels,
and managing Plotly figure interactivity.
"""

import json
import numpy as np
import pandas as pd
import geopandas as gpd
from typing import List, Tuple, Dict, Any

def labels_from_bins(bins: np.ndarray) -> List[str]:
    """
    Build readable class labels from ascending bin edges (exclusive left, inclusive right).
    For bins = [b1, b2, ..., bk], labels are:
      '≤ b1', '(b1, b2]', '(b2, b3]', ..., '(b_{k-1}, b_k]'
    """
    if len(bins) == 0:
        return []
    
    labels = [f"≤ {bins[0]:.2f}"]
    for a, b in zip(bins[:-1], bins[1:]):
        labels.append(f"({a:.2f}, {b:.2f}]")
    return labels


def visibility_mask(num_vars: int, num_years: int, var_idx: int, year_idx: int) -> List[bool]:
    """
    Helper for Plotly sliders/dropdowns.
    Generates a boolean list where only the trace at (var_idx, year_idx) is True.
    """
    mask = [False] * (num_vars * num_years)
    # The linear index in Plotly traces is usually: var_0_year_0, var_0_year_1, ... var_1_year_0...
    mask[var_idx * num_years + year_idx] = True
    return mask


def prepare_geodata(
    gdf: gpd.GeoDataFrame, 
    id_col: str, 
    year_col: str, 
    vars_to_show: List[str]
) -> Tuple[gpd.GeoDataFrame, Dict[str, Any], List[str], List[int]]:
    """
    Prepares a GeoDataFrame for Plotly ChoroplethMap.

    Steps:
    1. Filters columns and removes rows with missing IDs/Geometry.
    2. Projects to EPSG:4326 (Required by web maps).
    3. Extracts a distinct GeoJSON object (geometry) separate from the data.
    4. Sorts years numerically.

    Returns:
        tuple: (processed_gdf, geojson_dict, location_ids_list, sorted_years_list)
    """
    # 1. Filter columns to minimize memory usage
    # Defensive check: ensure requested vars actually exist
    available_vars = [c for c in vars_to_show if c in gdf.columns]
    cols_needed = [year_col, id_col, "geometry"] + available_vars
    
    # Copy to avoid mutating the original
    gdf_clean = gdf[cols_needed].dropna(subset=[id_col, "geometry"]).copy()

    # 2. Ensure Lat/Lon projection (EPSG:4326)
    if gdf_clean.crs != "EPSG:4326":
        gdf_clean = gdf_clean.to_crs("EPSG:4326")

    # Standardize ID to string to avoid JSON key issues
    gdf_clean[id_col] = gdf_clean[id_col].astype(str)

    # 3. Extract Unique Years
    # Handle mixed types safely, converting to int for sorting
    raw_years = gdf_clean[year_col].unique()
    try:
        years = sorted([int(y) for y in raw_years])
    except (ValueError, TypeError):
        # Fallback for string years
        years = sorted(list(raw_years))

    # 4. Extract Base GeoJSON
    # Plotly is faster if we give it one GeoJSON with unique geometries, 
    # rather than duplicating geometry for every year in the dataframe.
    geo_base = (
        gdf_clean[[id_col, "geometry"]]
        .drop_duplicates(subset=[id_col])
        .set_index(id_col)
        .sort_index()
    )

    # Convert to JSON dict
    geojson = json.loads(geo_base.reset_index().to_json())
    
    # List of IDs in the same order as the GeoJSON features
    locs = geo_base.index.tolist()

    return gdf_clean, geojson, locs, years