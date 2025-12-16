"""
AtlasBR - Application Layer for Census Data.

Orchestrates fetching geometries, retrieving attributes (BD/FTP),
and performing spatial operations (Clipping, H3 Interpolation).
"""

import pandas as pd
import geopandas as gpd
from typing import List, Literal, Optional

# --- Internal Modules ---
from atlasbr.core.catalog.census import get_theme_spec
from atlasbr.core.logic import census as logic
from atlasbr.infra.adapters import census_bd, census_ftp
from atlasbr.infra.geo import tracts as infra_tracts
from atlasbr.infra.geo import footprint as infra_urban
from atlasbr.infra.geo import resolver
from atlasbr.core.geo import ops as geo_ops
from atlasbr.core.geo import h3 as geo_spatial
from atlasbr.settings import get_billing_id, logger
from atlasbr.core.types import PlaceInput, CensusTheme, GeoGranularity

# --- Dispatch Registry ---
_TRANSFORM_REGISTRY = {
    ("basic", 2010): logic.harmonize_basic_2010,
    ("income", 2010): logic.harmonize_income_2010,
    ("age", 2010): logic.process_age_2010,
    ("race", 2010): logic.process_race_2010,
    ("age", 2022): logic.process_age_2022,
    ("race", 2022): logic.process_race_2022,
    ("basic", 2022): lambda df: df.rename(columns={"pessoas": "habitantes"}),
}


def load_census(
    places: List[PlaceInput],
    *,
    year: int = 2010,
    themes: List[CensusTheme] = ["basic", "income"],
    gcp_billing: Optional[str] = None,
    strategy: str,
    clip_urban: bool = True,
    geometry: GeoGranularity = "tract",
    h3_res: int = 8,
) -> gpd.GeoDataFrame:
    """
    Main entry point to load Census data.

    Args:
        places: List of municipalities (IDs or "Name, UF").
        year: Census year (2010 or 2022).
        themes: List of themes to load.
        gcp_billing: Google Cloud Project ID (overrides env var).
        clip_urban: If True, clips tracts to the urban footprint.
        geometry: Output format ("tract" or "h3").
        h3_res: H3 resolution level (if geometry="h3").

    Returns:
        gpd.GeoDataFrame: Indexed by 'id_setor_censitario' or 'h3_index'.
    """
    # 0. Configuration
    project_id = gcp_billing or settings.get_billing_id()

    # 1. Resolve Inputs
    muni_ids = resolver.resolve_places_to_ids(places)
    logger.info(
        f"🔄 Resolved {len(places)} inputs into {len(muni_ids)} unique municipalities."
    )

    # 2. Load & Prepare Geometry
    raw_tracts = infra_tracts.fetch_tracts_raw(muni_ids, year)
    gdf = geo_ops.prepare_tracts(raw_tracts)

    # 3. Clip to Urban Footprint
    if clip_urban:
        logger.info(f"    ✂️  Clipping to Urban Area...")
        raw_urban = infra_urban.fetch_urban_area_raw_gdf(year)
        urban_mask = geo_ops.create_urban_mask(
            raw_urban, bbox=gdf.total_bounds, target_crs=str(gdf.crs)
        )
        gdf = geo_ops.clip_to_mask(gdf, urban_mask)
        logger.info(f"       -> Retained {len(gdf)} tracts after clip.")

    # 4. Iterate & Load Themes
    for theme in themes:
        logger.info(f"    📦 Loading theme: '{theme}'...")

        try:
            spec = get_theme_spec(theme, year, strategy)
        except ValueError as e:
            logger.warning(f"        ⚠️ Warning: {e}. Skipping.")
            continue

        # Strategy Dispatch
        if spec.strategy == "bd_table":
            df_raw = census_bd.fetch_from_bd(
                spec.table_id, spec.required_columns, muni_ids, project_id
            )
        elif spec.strategy == "ftp_csv":
            df_raw = census_ftp.fetch_census_ftp(spec)
        else:
            logger.warning(f"        ⚠️ Unknown strategy '{spec.strategy}'. Skipping.")
            continue

        # Transform
        transform_func = _TRANSFORM_REGISTRY.get((theme, year))
        if transform_func:
            df_clean = transform_func(df_raw)
        else:
            df_clean = df_raw

        # Merge
        gdf = gdf.join(df_clean, how="left")

    # 5. H3 Aggregation (Optional)
    if geometry == "h3":
        logger.info(f"    ⬢  Aggregating to H3 Grid (Res {h3_res})...")

        # A. Generate H3 Grid covering the current Tracts
        gdf_h3 = geo_spatial.h3fy(
            source=gdf,
            resolution=h3_res,
            buffer=True,  # Ensure edges are covered
            clip=False,  # Do not hard-clip hexes for interpolation accuracy
        )

        # B. Define Variable Types for Areal Weighting
        all_cols = [
            c for c in gdf.columns if c not in ["geometry", "id_setor_censitario"]
        ]
        # Heuristic: Rates/Means are Intensive, Counts/Totals are Extensive
        intensive = [
            c
            for c in all_cols
            if any(x in c for x in ["rendimento", "taxa", "mean", "rate"])
        ]
        extensive = [c for c in all_cols if c not in intensive]

        # C. Interpolate
        gdf = geo_spatial.interpolate_area_weighted(
            source_gdf=gdf,
            target_gdf=gdf_h3,
            extensive_vars=extensive,
            intensive_vars=intensive,
        )
        logger.info(f"       -> Interpolated data to {len(gdf)} hexagons.")

    # 6. Final Polish
    gdf["year"] = year
    logger.info(f"✅ Loaded Census {year} for {len(muni_ids)} municipalities.")

    return gdf
