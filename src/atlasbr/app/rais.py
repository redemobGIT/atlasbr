"""
AtlasBR - Application Layer for RAIS (Employment) Data.

Handles the "hybrid pipeline" that can inject public sector jobs
(from Schools/Hospitals) into the main RAIS dataset.
"""

from __future__ import annotations

from typing import Optional, Union

import geopandas as gpd
import pandas as pd

from atlasbr.core.catalog.rais import get_rais_spec
from atlasbr.core.logic import geocoding, integration
from atlasbr.core.types import PlaceInput
from atlasbr.infra.geo import resolver
from atlasbr.settings import logger, resolve_billing_id


def load_rais(
    places: list[PlaceInput],
    *,
    year: int = 2021,
    strategy: str = bd,
    gcp_billing: Optional[str] = None,
    geocode: bool = False,
    include_public_sector: bool = False,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Loads RAIS Establishment data, optionally enriching it with public sector
    data.

    Args:
        places: List of municipalities.
        year: Year of the dataset.
        strategy: 'bd_table' (BigQuery) or 'ftp_csv' (not implemented).
        gcp_billing: Google Cloud Project ID.
        geocode: If True, attaches coordinates based on CEP.
        include_public_sector: If True, fetches Schools (INEP) and Health
            Units (CNES) and merges them to fill gaps in RAIS public
            administration coverage.
    """
    project_id = resolve_billing_id(gcp_billing) if strategy == "bd" else None
    muni_ids = resolver.resolve_places_to_ids(places)
    spec = get_rais_spec(year, strategy)

    if spec.strategy == "bd":
        from atlasbr.infra.adapters import rais_bd

        logger.info(f"    🏭 Loading RAIS {year} via strategy {strategy!r}...")
        main_dataset = rais_bd.fetch_rais_from_bd(
            spec,
            munis=muni_ids,
            year=year,
            billing_id=project_id,
        )
    else:
        raise NotImplementedError(
            f"Strategy {strategy!r} is defined in catalog but not implemented."
        )

    if include_public_sector:
        logger.info("    🧩 Injecting public sector data (Schools + Health)...")

        schools = pd.DataFrame()
        health = pd.DataFrame()

        try:
            from atlasbr.app.inep import load_schools

            schools = load_schools(
                places=places,
                year=year,
                gcp_billing=project_id,
                as_gdf=False,
            )
        except Exception as exc:
            logger.warning(
                f"Failed to load Schools for {year}: {exc}. Skipping injection."
            )

        try:
            from atlasbr.app.cnes import load_cnes

            health = load_cnes(
                places=places,
                year=year,
                month=9,
                gcp_billing=project_id,
                geocode=False,
            )
        except Exception as exc:
            logger.warning(
                f"Failed to load CNES for {year}: {exc}. Skipping injection."
            )

        schools_h = integration.harmonize_schools_to_rais(schools)
        health_h = integration.harmonize_cnes_to_rais(health)

        if not schools_h.empty and "id_escola" in schools_h.columns:
            schools_h = schools_h.rename(
                columns={"id_escola": "id_estab_original"}
            )

        if (
            not health_h.empty
            and "id_estabelecimento_cnes" in health_h.columns
        ):
            health_h = health_h.rename(
                columns={"id_estabelecimento_cnes": "id_estab_original"}
            )

        to_merge: list[pd.DataFrame] = [main_dataset]
        if not schools_h.empty:
            to_merge.append(schools_h)
        if not health_h.empty:
            to_merge.append(health_h)

        if len(to_merge) > 1:
            main_dataset = pd.concat(to_merge, ignore_index=True)
            logger.info(
                "       -> Integrated %d schools and %d health units.",
                len(schools_h),
                len(health_h),
            )

    if geocode:
        from atlasbr.infra.adapters import ceps_bd

        df_ceps = ceps_bd.fetch_ceps_from_bd(
            munis=muni_ids,
            billing_id=project_id,
        )

        logger.info(
            f"    🌍 Geocoding {len(main_dataset)} establishments via CEP..."
        )
        gdf_rais = geocoding.geocode_by_cep(
            data_df=main_dataset,
            cep_df=df_ceps,
            data_cep_col="cep",
        )
        logger.info(f"✅ Loaded {len(gdf_rais)} establishments (Geolocated).")
        return gdf_rais

    logger.info(f"✅ Loaded {len(main_dataset)} establishments (Tabular).")
    return main_dataset
