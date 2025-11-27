"""
AtlasBR - Application Layer for Census Data.
"""
import pandas as pd
import geopandas as gpd
from typing import List, Literal

from atlasbr.core.catalog.census import get_theme_spec
from atlasbr.core.logic import census as logic
from atlasbr.infra.adapters import census_bd, census_ftp
from atlasbr.infra.geo import tracts as infra_tracts
from atlasbr.infra.geo import footprint as infra_urban
from atlasbr.infra.geo import resolver
from atlasbr.core.geo import ops as geo_ops
from atlasbr.core.geo import h3 as geo_spatial
from atlasbr.settings import logger
from atlasbr.core.types import PlaceInput, CensusTheme, GeoGranularity

# Dispatch Registry
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
    gcp_billing: str,
    clip_urban: bool = True,
    geometry: GeoGranularity = "tract",
    h3_res: int = 8,
) -> gpd.GeoDataFrame:
    """
    Main entry point to load Census data.
    """
    # 1. Resolve
    muni_ids = resolver.resolve_places_to_ids(places)
    logger.info(f"🔄 Resolved {len(places)} inputs into {len(muni_ids)} unique municipalities.")

    # 2. Geometry
    raw_tracts = infra_tracts.fetch_tracts_raw(muni_ids, year)
    gdf = geo_ops.prepare_tracts(raw_tracts)
    
    # 3. Clip
    if clip_urban:
        logger.info(f"    ✂️  Clipping to Urban Area...")
        raw_urban = infra_urban.fetch_urban_area_raw_gdf(year)
        urban_mask = geo_ops.create_urban_mask(raw_urban, gdf.total_bounds, str(gdf.crs))
        gdf = geo_ops.clip_to_mask(gdf, urban_mask)
        logger.info(f"       -> Retained {len(gdf)} tracts after clip.")

    # 4. Themes
    for theme in themes:
        logger.info(f"    📦 Loading theme: '{theme}'...")
        try:
            spec = get_theme_spec(theme, year)
        except ValueError as e:
            logger.warning(f"        ⚠️ Warning: {e}. Skipping.")
            continue

        if spec.strategy == "bd_table":
            df_raw = census_bd.fetch_from_bd(
                spec.table_id, spec.required_columns, muni_ids, gcp_billing
            )
        elif spec.strategy == "ftp_csv":
            df_raw = census_ftp.fetch_income_ftp_2022(spec.url)
        else:
            logger.warning(f"        ⚠️ Unknown strategy '{spec.strategy}'. Skipping.")
            continue

        transform_func = _TRANSFORM_REGISTRY.get((theme, year))
        df_clean = transform_func(df_raw) if transform_func else df_raw
        gdf = gdf.join(df_clean, how="left")

    # 5. H3
    if geometry == "hex":
        logger.info(f"    ⬢  Aggregating to H3 Grid (Res {h3_res})...")
        bbox_ll = gdf.to_crs("EPSG:4326").total_bounds
        gdf_h3 = geo_spatial.generate_h3_grid(bbox_ll, h3_res, str(gdf.crs))
        
        all_cols = [c for c in gdf.columns if c not in ["geometry", "id_setor_censitario"]]
        intensive = [c for c in all_cols if any(x in c for x in ["rendimento", "taxa", "mean", "rate"])]
        extensive = [c for c in all_cols if c not in intensive]
        
        gdf = geo_spatial.interpolate_area_weighted(gdf, gdf_h3, extensive, intensive)
        logger.info(f"       -> Interpolated data to {len(gdf)} hexagons.")

    gdf["year"] = year
    logger.info(f"✅ Loaded Census {year} for {len(muni_ids)} municipalities.")
    return gdf