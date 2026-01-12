"""
AtlasBR - Application Layer for CNES.
"""

from __future__ import annotations

from typing import Optional, Union

import geopandas as gpd
import pandas as pd

from atlasbr.core.catalog.cnes import get_cnes_spec
from atlasbr.core.logic import geocoding
from atlasbr.core.types import PlaceInput
from atlasbr.infra.geo import resolver
from atlasbr.settings import logger, resolve_billing_id


def load_cnes(
    places: list[PlaceInput],
    *,
    year: int = 2023,
    month: int = 9,
    gcp_billing: Optional[str] = None,
    geocode: bool = False,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    project_id = resolve_billing_id(gcp_billing)
    muni_ids = resolver.resolve_places_to_ids(places)
    spec = get_cnes_spec(year, month)

    from atlasbr.infra.adapters import cnes_bd

    df_cnes = cnes_bd.fetch_cnes_from_bd(
        spec,
        munis=muni_ids,
        year=year,
        month=month,
        billing_id=project_id,
    )

    if geocode:
        from atlasbr.infra.adapters import ceps_bd

        df_ceps = ceps_bd.fetch_ceps_from_bd(
            munis=muni_ids,
            billing_id=project_id,
        )

        logger.info(f"    🌍 Geocoding {len(df_cnes)} healthcare units via CEP...")
        gdf_cnes = geocoding.geocode_by_cep(
            data_df=df_cnes,
            cep_df=df_ceps,
            data_cep_col="cep",
        )
        logger.info(f"✅ Loaded {len(gdf_cnes)} CNES units (Geolocated).")
        return gdf_cnes

    logger.info(f"✅ Loaded {len(df_cnes)} CNES units (Tabular).")
    return df_cnes
