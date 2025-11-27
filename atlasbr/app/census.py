"""
AtlasBR - Application Layer for Census Data.

This module acts as the orchestrator. It ties together:
1. Infrastructure Adapters (Fetching Data & Geometries)
2. Core Logic (Transformations & Math)
3. Geo Operations (Clipping, H3 Interpolation)

It produces the final, ready-to-use GeoDataFrame.
"""

import pandas as pd
import geopandas as gpd
from typing import List, Union, Tuple, Literal

# --- Internal Modules ---

# 1. Catalog & Domain Logic
from atlasbr.core.catalog.census import get_theme_spec
from atlasbr.core.logic import census as logic

# 2. Data Adapters (Infra)
from atlasbr.infra.adapters import census_bd, census_ftp

# 3. Geo Adapters (Infra) -> Fetching things
from atlasbr.infra.geo import tracts as infra_tracts
from atlasbr.infra.geo import footprint as infra_urban
from atlasbr.infra.geo import resolver

# 4. Geo Logic (Core) -> Calculating things
from atlasbr.core.geo import ops as geo_ops
from atlasbr.core.geo import h3 as geo_spatial

# --- Type Definitions ---
PlaceInput = Union[int, str, Tuple[str, str]]
GeometryType = Literal["tracts", "h3"]

# --- Dispatch Registry ---
# Maps (theme, year) to the specific transformation function in core.logic.
# Missing keys imply pass-through (no transformation).
_TRANSFORM_REGISTRY = {
    # 2010
    ("basic", 2010): logic.harmonize_basic_2010,
    ("income", 2010): logic.harmonize_income_2010,
    ("age", 2010): logic.process_age_2010,
    ("race", 2010): logic.process_race_2010,
    
    # 2022
    ("age", 2022): logic.process_age_2022,
    ("race", 2022): logic.process_race_2022,
    # Basic 2022 needs a simple rename
    ("basic", 2022): lambda df: df.rename(columns={"pessoas": "habitantes"}),
}

def load_census(
    places: List[PlaceInput],
    *,
    year: int = 2010,
    themes: List[str] = ["basic", "income"],
    gcp_billing: str,
    clip_urban: bool = True,
    geometry: GeometryType = "tracts",
    h3_res: int = 8,
) -> gpd.GeoDataFrame:
    """
    Main entry point to load Census data.

    Orchestrates the fetching of geometries, attribute data, and optional H3 aggregation.

    Args:
        places: List of municipalities. Accepted formats:
                - IDs: 3304557 or "3304557"
                - Tuples: ("Rio de Janeiro", "RJ")
                - Strings: "Niterói, RJ"
        year: Census year (2010 or 2022).
        themes: List of themes (e.g., 'basic', 'age', 'race', 'income').
        gcp_billing: Google Cloud Project ID for BigQuery.
        clip_urban: If True, clips census tracts to the urbanized area.
        geometry: Output geometry type. "tracts" (default) or "h3".
        h3_res: H3 Resolution (if geometry="h3"). Default 8 (~0.7km²).

    Returns:
        gpd.GeoDataFrame: Indexed by 'id_setor_censitario' (if tracts) or 'h3_index'.
    """
    
    # 1. Resolve Inputs
    # --------------------------------------------------------------------------
    muni_ids = resolver.resolve_places_to_ids(places)
    print(f"🔄 Resolved {len(places)} inputs into {len(muni_ids)} unique municipalities.")

    # 2. Load & Prepare Geometry (The Backbone)
    # --------------------------------------------------------------------------
    # A. Fetch Raw Tracts (Infra)
    raw_tracts = infra_tracts.fetch_tracts_raw(muni_ids, year)
    
    # B. Clean & Project to UTM (Core Logic)
    gdf = geo_ops.prepare_tracts(raw_tracts)
    
    # 3. Clip to Urban Footprint (Optional)
    # --------------------------------------------------------------------------
    if clip_urban:
        print(f"    ✂️  Clipping to Urban Area...")
        # A. Fetch Raw Urban Shapes (Infra)
        raw_urban = infra_urban.fetch_urban_area_raw_gdf(year)
        
        # B. Calculate Mask (Core Logic)
        urban_mask = geo_ops.create_urban_mask(
            raw_urban, 
            bbox=gdf.total_bounds, 
            target_crs=str(gdf.crs)
        )
        
        # C. Apply Clip (Core Logic)
        gdf = geo_ops.clip_to_mask(gdf, urban_mask)
        print(f"       -> Retained {len(gdf)} tracts after clip.")

    # 4. Iterate & Load Themes
    # --------------------------------------------------------------------------
    for theme in themes:
        print(f"    📦 Loading theme: '{theme}'...")
        
        try:
            spec = get_theme_spec(theme, year)
        except ValueError as e:
            print(f"        ⚠️ Warning: {e}. Skipping.")
            continue

        # A. Fetch Raw Data (Strategy Dispatch via Adapters)
        if spec.strategy == "bd_table":
            df_raw = census_bd.fetch_from_bd(
                table_id=spec.table_id,
                columns=spec.required_columns,
                munis=muni_ids,
                billing_id=gcp_billing
            )
        elif spec.strategy == "ftp_csv":
            df_raw = census_ftp.fetch_income_ftp_2022(spec.url)
        else:
            print(f"        ⚠️ Unknown strategy '{spec.strategy}'. Skipping.")
            continue

        # B. Apply Transformation Logic (Core Logic)
        transform_func = _TRANSFORM_REGISTRY.get((theme, year))
        if transform_func:
            df_clean = transform_func(df_raw)
        else:
            df_clean = df_raw

        # C. Merge into Geometry
        gdf = gdf.join(df_clean, how="left")

    # 5. Optional: H3 Aggregation
    # --------------------------------------------------------------------------
    if geometry == "h3":
        print(f"    ⬢  Aggregating to H3 Grid (Res {h3_res})...")
        
        # A. Generate Target Grid (Core Logic)
        # Convert bounds to 4326 for H3 generation, then back to local UTM
        bbox_ll = gdf.to_crs("EPSG:4326").total_bounds
        gdf_h3 = geo_spatial.generate_h3_grid(
            bbox=bbox_ll, 
            resolution=h3_res, 
            crs=str(gdf.crs)
        )
        
        # B. Identify Variable Types for Tobler
        # Heuristic: 'rendimento' and rates are intensive (mean), others extensive (sum)
        all_cols = [c for c in gdf.columns if c not in ["geometry", "id_setor_censitario"]]
        intensive = [c for c in all_cols if any(x in c for x in ["rendimento", "taxa", "mean", "rate"])]
        extensive = [c for c in all_cols if c not in intensive]
        
        # C. Interpolate (Core Logic - Tobler Wrapper)
        gdf = geo_spatial.interpolate_area_weighted(
            source_gdf=gdf,
            target_gdf=gdf_h3,
            extensive_vars=extensive,
            intensive_vars=intensive
        )
        print(f"       -> Interpolated data to {len(gdf)} hexagons.")

    # 6. Final Polish
    # --------------------------------------------------------------------------
    gdf["year"] = year
    print(f"✅ Loaded Census {year} for {len(muni_ids)} municipalities.")
    
    return gdf