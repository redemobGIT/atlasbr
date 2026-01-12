"""
AtlasBR - Application Layer for Schools.
"""

from __future__ import annotations

from typing import Optional, Union

import geopandas as gpd
import pandas as pd

from atlasbr.core.catalog.inep import get_schools_spec
from atlasbr.core.logic import geocoding
from atlasbr.core.types import PlaceInput
from atlasbr.infra.geo import resolver
from atlasbr.settings import logger, resolve_billing_id


def load_schools(
    places: list[PlaceInput],
    *,
    year: int = 2023,
    gcp_billing: Optional[str] = None,
    as_gdf: bool = True,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    project_id = resolve_billing_id(gcp_billing)
    muni_ids = resolver.resolve_places_to_ids(places)
    spec = get_schools_spec(year)

    from atlasbr.infra.adapters import inep_bd

    df_schools = inep_bd.fetch_schools_from_bd(
        spec,
        munis=muni_ids,
        year=year,
        billing_id=project_id,
    )

    if as_gdf:
        logger.info(f"    🌍 Converting {len(df_schools)} schools to geometry...")
        gdf_schools = geocoding.points_from_coords(
            df_schools,
            lat_col="latitude",
            lon_col="longitude",
        )
        logger.info(f"✅ Loaded {len(gdf_schools)} schools.")
        return gdf_schools

    logger.info(f"✅ Loaded {len(df_schools)} schools (Tabular).")
    return df_schools
