"""
AtlasBR - Application Layer for Census Data.

Orchestrates the loading of tabular data (BD/FTP) and spatial geometries
(Tracts/H3) to produce ready-to-use GeoDataFrames.
"""
import pandas as pd
import geopandas as gpd
from typing import List, Union, Optional

from atlasbr.core.catalog.census import get_census_spec
from atlasbr.infra.geo import resolver, tracts, footprint
from atlasbr.core.geo import ops, h3 as h3_ops
from atlasbr.core.logic import census as census_logic
from atlasbr.settings import logger, resolve_billing_id
from atlasbr.core.types import PlaceInput


def load_census(
    places: List[PlaceInput],
    *,
    year: int = 2010,
    themes: List[str] = None,
    strategy: str = "bd_table",
    geometry: str = "tract",  # 'tract' or 'h3'
    h3_res: int = 8,
    gcp_billing: Optional[str] = None,
    clip_urban: bool = False,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Loads Census data for the requested places and themes.

    Orchestrates fetching (Infra), normalization (Logic), and
    spatial operations (Core).

    Args:
        places: List of municipality IDs (e.g., 3304557) or names.
        year: Census year (2010 or 2022).
        themes: List of themes to load (e.g. ['basic', 'income']).
                Defaults to ['basic'].
        strategy: 'bd_table' (BigQuery) or 'ftp_csv' (Offline).
        geometry: Output geometry 'tract' (original) or 'h3' (hex grid).
        h3_res: H3 resolution if geometry='h3'.
        gcp_billing: Google Cloud Project ID (required for 'bd_table').
        clip_urban: If True, clips result to the urban footprint.

    Returns:
        GeoDataFrame with requested census variables.
    """
    if themes is None:
        themes = ["basic"]

    # 1. Resolve Inputs and Billing
    project_id = (
        resolve_billing_id(gcp_billing) if strategy == "bd_table" else None
    )
    muni_ids = resolver.resolve_places_to_ids(places)

    if not muni_ids:
        err_msg = f"Could not resolve any municipalities from: {places}"
        raise ValueError(err_msg)

    # 2. Load and Normalize Data (Iterate themes)
    merged_df = pd.DataFrame()
    extensive_vars: List[str] = []
    intensive_vars: List[str] = []

    for theme in themes:
        spec = get_census_spec(year, theme, strategy)
        logger.info(
            f"    📦 Loading theme: '{theme}' via {strategy}..."
        )

        try:
            # A. Fetch Raw Data
            if spec.strategy == "bd_table":
                from atlasbr.infra.adapters import census_bd
                df_raw = census_bd.fetch_census_bd(
                    spec, munis=muni_ids, billing_id=project_id
                )
            elif spec.strategy == "ftp_csv":
                from atlasbr.infra.adapters import census_ftp
                df_raw = census_ftp.fetch_census_ftp(spec, munis=muni_ids)
            else:
                raise NotImplementedError(
                    f"Strategy {strategy} not implemented."
                )

            # B. Apply Logic (Transformation Layer)
            # This handles race imputation, residuals, and type enforcement.
            df_clean = census_logic.standardize_census_dataframe(
                df_raw, spec
            )

            # C. Collect Metadata for Aggregation
            # Assumes spec has these properties defined in Catalog
            if hasattr(spec, "extensive_vars"):
                extensive_vars.extend(spec.extensive_vars)
            if hasattr(spec, "intensive_vars"):
                intensive_vars.extend(spec.intensive_vars)

            # D. Merge
            if merged_df.empty:
                merged_df = df_clean
            else:
                merged_df = merged_df.join(
                    df_clean, how="outer", rsuffix=f"_{theme}"
                )

        except Exception as e:
            logger.error(f"Failed to load theme '{theme}': {e}")
            raise

    if merged_df.empty:
        raise RuntimeError("No census data found for requested criteria.")

    # 3. Handle Geometry (Tracts)
    logger.info("    🗺️  Fetching Tract Geometries...")

    raw_tracts = tracts.fetch_tracts_raw(munis=muni_ids, year=year)
    gdf_tracts = ops.prepare_tracts(raw_tracts)

    # 4. Join Data + Geometry
    # We use inner join to ensure we only return geometries with data.
    # However, we log if this results in significant data loss.
    initial_rows = len(merged_df)
    gdf_data = gdf_tracts.join(merged_df, how="inner")
    final_rows = len(gdf_data)

    if final_rows == 0:
        raise RuntimeError(
            "Intersection of Census Data and Geometries is empty. "
            "Check if year/municipality codes align."
        )

    if final_rows < initial_rows:
        dropped = initial_rows - final_rows
        logger.warning(
            f"    ⚠️ Dropped {dropped} data rows due to missing "
            "geometries. This is common if Malha is versioned "
            "differently from Census data."
        )

    # 5. Optional: Urban Clipping
    if clip_urban:
        logger.info("    ✂️  Clipping to Urban Area...")
        urban_mask = footprint.fetch_urban_area_raw_gdf(year)
        
        # Optimize: Create mask only for the ROI bounding box
        roi_bbox = gdf_data.total_bounds
        local_mask = ops.create_urban_mask(
            urban_mask, roi_bbox, gdf_data.crs
        )
        gdf_data = ops.clip_to_mask(gdf_data, local_mask)

    # 6. Spatial Aggregation (H3)
    if geometry == "h3":
        logger.info(f"    ⬢ Aggregating to H3 resolution {h3_res}...")

        # A. Create target hex grid
        gdf_hex = h3_ops.h3fy(gdf_data, resolution=h3_res, clip=True)

        # B. Filter vars that actually exist in the dataframe
        cols = merged_df.columns
        valid_ext = [c for c in extensive_vars if c in cols]
        valid_int = [c for c in intensive_vars if c in cols]

        if not valid_ext and not valid_int:
            logger.warning(
                "    ⚠️ No aggregation rules found for loaded columns. "
                "H3 output will only contain geometry."
            )

        # C. Interpolate
        interpolated = h3_ops.interpolate_area_weighted(
            source_gdf=gdf_data,
            target_gdf=gdf_hex,
            extensive_vars=valid_ext,
            intensive_vars=valid_int,
            preserve_totals=True
        )

        # Clean up geometry column if duplicated during join
        if "geometry" in interpolated.columns:
            interpolated = interpolated.drop(columns="geometry")
            
        return gdf_hex.join(interpolated)

    return gdf_data