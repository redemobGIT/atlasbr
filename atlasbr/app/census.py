"""
AtlasBR - Application Layer for Census Data.

This module acts as the orchestrator. It ties together the Infrastructure adapters
(fetching data), the Core logic (transforming data), and the Geo loader (spatial operations)
to produce the final, ready-to-use GeoDataFrame.
"""

import pandas as pd
import geopandas as gpd
from typing import List, Union, Tuple

# Internal Modules
from atlasbr.core.catalog.census import get_theme_spec
from atlasbr.core.logic import census as logic
from atlasbr.infra.adapters import census_bd, census_ftp
from atlasbr.geo import tracts, urban, resolver

# Type alias matching the resolver
PlaceInput = Union[int, str, Tuple[str, str]]

# --- Dispatch Registry ---
# Maps (theme, year) to the specific transformation function in core.logic.
# If a key is missing, it implies no transformation is needed (pass-through).
_TRANSFORM_REGISTRY = {
    # 2010
    ("basic", 2010): logic.harmonize_basic_2010,
    ("income", 2010): logic.harmonize_income_2010,
    ("age", 2010): logic.process_age_2010,
    ("race", 2010): logic.process_race_2010,
    
    # 2022
    ("age", 2022): logic.process_age_2022,
    ("race", 2022): logic.process_race_2022,
    # Basic 2022 needs a simple rename: 'pessoas' -> 'habitantes'
    ("basic", 2022): lambda df: df.rename(columns={"pessoas": "habitantes"}),
}

def load_census(
    places: List[PlaceInput],
    *,
    year: int = 2010,
    themes: List[str] = ["basic", "income"],
    gcp_billing: str,
    clip_urban: bool = True,
) -> gpd.GeoDataFrame:
    """
    Main entry point to load Census data.

    Orchestrates the fetching of geometries and attribute data, harmonizes column names,
    and optionally clips the result to the urban footprint.

    Args:
        places: List of municipalities. Accepted formats:
                - IDs: 3304557 or "3304557"
                - Tuples: ("Rio de Janeiro", "RJ")
                - Strings: "Niterói, RJ"
        year: Census year (2010 or 2022).
        themes: List of themes to load (e.g., 'basic', 'age', 'race', 'income').
        gcp_billing: Google Cloud Project ID for BigQuery.
        clip_urban: If True, clips census tracts to the urbanized area of the municipality.

    Returns:
        gpd.GeoDataFrame: Tracts with joined attributes, indexed by 'id_setor_censitario'.
    """
    
    # 0. Resolve Inputs (The new robust step)
    # --------------------------------------------------------------------------
    # Converts names ("Rio, RJ") into official 7-digit IDs (3304557)
    muni_ids = resolver.resolve_places(places)
    print(f"🔄 Resolved {len(places)} inputs into {len(muni_ids)} unique municipalities.")

    # 1. Load Geometry (The Backbone)
    # --------------------------------------------------------------------------
    gdf = tracts.load_tracts(muni_ids, year)
    
    # 2. Clip to Urban Footprint (Optional)
    # --------------------------------------------------------------------------
    if clip_urban:
        print(f"    ✂️  Clipping to Urban Area...")
        # Get the mask for the bounding box of our loaded tracts
        urban_mask = urban.get_urban_mask(
            year=year,
            target_crs=str(gdf.crs),
            bbox=gdf.total_bounds
        )
        
        # Spatial Overlay (Intersection)
        # Reset index is needed because overlay discards the index
        gdf = gpd.overlay(
            gdf.reset_index(), 
            urban_mask, 
            how="intersection"
        ).set_index("id_setor_censitario")
        
        # Clean up empty/invalid geometries after clip
        gdf = gdf[~gdf.is_empty & gdf.geometry.is_valid]

    # 3. Iterate & Load Themes
    # --------------------------------------------------------------------------
    for theme in themes:
        print(f"    📦 Loading theme: '{theme}'...")
        
        # A. Get the Contract (Spec)
        try:
            spec = get_theme_spec(theme, year)
        except ValueError as e:
            print(f"        ⚠️ Warning: {e}. Skipping.")
            continue

        # B. Fetch Raw Data (Strategy Dispatch)
        # ---------------------------------------
        if spec.strategy == "bd_table":
            df_raw = census_bd.fetch_from_bd(
                table_id=spec.table_id,
                columns=spec.required_columns,
                munis=muni_ids,  # Pass the resolved IDs
                billing_id=gcp_billing
            )
        elif spec.strategy == "ftp_csv":
            df_raw = census_ftp.fetch_income_ftp_2022(spec.url)
        else:
            print(f"        ⚠️ Unknown strategy '{spec.strategy}'. Skipping.")
            continue

        # C. Apply Transformation Logic
        # ---------------------------------------
        # Look up the processor function for this specific (theme, year)
        transform_func = _TRANSFORM_REGISTRY.get((theme, year))
        
        if transform_func:
            df_clean = transform_func(df_raw)
        else:
            df_clean = df_raw  # Pass-through if no logic defined

        # D. Merge into Geometry
        # ---------------------------------------
        # We use a left join to keep the geometries even if data is missing for some tracts
        gdf = gdf.join(df_clean, how="left")

    # 4. Final Polish
    # --------------------------------------------------------------------------
    gdf["year"] = year
    # Ensure MultiIndex or standard columns as preferred. 
    # Current return: Index=id_setor, Col=year, geometry, ...
    
    print(f"✅ Loaded {len(gdf)} tracts with themes: {themes}")
    return gdf