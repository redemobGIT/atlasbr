"""
AtlasBR - Application Layer for Census Data.

Orchestrates the entire Census data pipeline:
1. Resolves municipality names/IDs to geometries.
2. Fetches attribute data (Demographics, Income) via lazy-loaded adapters (BD or FTP).
3. Standardizes columns to a canonical schema.
4. Performs optional spatial operations (Clipping to urban footprints, H3 aggregation).
"""

import pandas as pd
import geopandas as gpd
from typing import List, Optional

# --- Internal Modules ---
from atlasbr.core.catalog.census import get_theme_spec
from atlasbr.core.logic import census as logic
from atlasbr.infra.geo import tracts as infra_tracts
from atlasbr.infra.geo import footprint as infra_urban
from atlasbr.infra.geo import resolver
from atlasbr.core.geo import ops as geo_ops
from atlasbr.core.geo import h3 as geo_spatial
from atlasbr.settings import logger, resolve_billing_id
from atlasbr.core.types import PlaceInput, CensusTheme, GeoGranularity


def load_census(
    places: List[PlaceInput],
    *,
    year: int = 2010,
    themes: Optional[List[CensusTheme]] = None,
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
        themes: List of themes to load (default: ["basic", "income"]).
        gcp_billing: Google Cloud Project ID (overrides env var, only used for 'bd_table').
        strategy: Data source strategy ('bd_table' or 'ftp_csv').
        clip_urban: If True, clips tracts to the urban footprint (removes rural/empty areas).
        geometry: Output spatial unit ("tract" or "h3").
        h3_res: H3 resolution level (only used if geometry="h3").

    Returns:
        gpd.GeoDataFrame: Census data indexed by 'id_setor_censitario' or 'h3_index'.
    """
    if themes is None:
        themes = ["basic", "income"]

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
            raw_urban, 
            bbox=gdf.total_bounds, 
            target_crs=str(gdf.crs)
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

        # Strategy Dispatch (Lazy Imports)
        if spec.strategy == "bd_table":
            from atlasbr.infra.adapters import census_bd
            # Billing is only resolved if we actually hit BigQuery
            project_id = resolve_billing_id(gcp_billing)
            df_raw = census_bd.fetch_from_bd(
                spec.table_id, spec.required_columns, muni_ids, project_id
            )

        elif spec.strategy == "ftp_csv":
            from atlasbr.infra.adapters import census_ftp
            df_raw = census_ftp.fetch_census_ftp(spec)

        else:
            logger.warning(f"        ⚠️ Unknown strategy '{spec.strategy}'. Skipping.")
            continue

        # Standardize Columns (Enforce Canonical Schema)
        df_clean = logic.standardize_census_dataframe(df_raw, theme, year, strategy)

        # Merge
        gdf = gdf.join(df_clean, how="left")

    # 5. H3 Aggregation (Optional)
    if geometry == "h3":
        logger.info(f"    ⬢  Aggregating to H3 Grid (Res {h3_res})...")

        # A. Generate H3 Grid
        gdf_h3 = geo_spatial.h3fy(
            source=gdf,
            resolution=h3_res,
            buffer=True,
            clip=False,
        )

        # B. Define Variable Types for Areal Interpolation
        # Heuristic: Rates/Means are Intensive, Counts/Totals are Extensive
        # TODO: Move this configuration to the core logic/catalog in future refactors
        excluded_cols = {"geometry", "id_setor_censitario"}
        candidate_cols = [c for c in gdf.columns if c not in excluded_cols]
        
        intensive_keywords = {"rendimento", "taxa", "mean", "rate"}
        
        intensive_vars = [
            c for c in candidate_cols 
            if any(k in c for k in intensive_keywords)
        ]
        extensive_vars = [c for c in candidate_cols if c not in intensive_vars]

        # C. Interpolate
        gdf = geo_spatial.interpolate_area_weighted(
            source_gdf=gdf,
            target_gdf=gdf_h3,
            extensive_vars=extensive_vars,
            intensive_vars=intensive_vars,
        )
        logger.info(f"       -> Interpolated data to {len(gdf)} hexagons.")

    # 6. Final Polish
    gdf["year"] = year
    logger.info(f"✅ Loaded Census {year} for {len(muni_ids)} municipalities.")

    return gdf