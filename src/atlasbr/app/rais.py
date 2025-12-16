"""
AtlasBR - Application Layer for RAIS (Employment) Data.

Handles the 'Hybrid Pipeline' which can inject public sector jobs 
(from Schools/Hospitals) into the main RAIS dataset.
"""
import pandas as pd
import geopandas as gpd
from typing import List, Union, Optional

from atlasbr.core.catalog.rais import get_rais_spec
from atlasbr.core.logic import geocoding, integration
from atlasbr.infra.geo import resolver
from atlasbr.settings import logger, resolve_billing_id
from atlasbr.core.types import PlaceInput

def load_rais(
    places: List[PlaceInput],
    *,
    year: int = 2021,
    strategy: str = "bd_table",
    gcp_billing: Optional[str] = None,
    geocode: bool = False,
    include_public_sector: bool = False,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Loads RAIS Establishment data, optionally enriching it with Public Sector data.

    Args:
        places: List of municipalities.
        year: Year of the dataset.
        strategy: 'bd_table' (BigQuery) or 'ftp_csv' (not yet implemented).
        gcp_billing: Google Cloud Project ID.
        geocode: If True, attaches coordinates based on CEP.
        include_public_sector: If True, fetches Schools (INEP) and Health Units (CNES)
                               and merges them to fill gaps in RAIS public administration coverage.
    """
    # 1. Configuration
    project_id = resolve_billing_id(gcp_billing) if strategy == "bd_table" else None

    # 2. Resolve Inputs
    muni_ids = resolver.resolve_places_to_ids(places)
    
    # 3. Get Spec (Strict Dispatch)
    # Raises ValueError if the strategy/year combination is invalid
    spec = get_rais_spec(year, strategy)
    
    # 4. Fetch Main Data (RAIS)
    if spec.strategy == "bd_table":
        from atlasbr.infra.adapters import rais_bd
        
        logger.info(f"    🏭 Loading RAIS {year} via strategy '{strategy}'...")
        main_dataset = rais_bd.fetch_rais_from_bd(
            table_id=spec.table_id,
            columns=spec.required_columns,
            munis=muni_ids,
            year=year,
            billing_id=project_id
        )
    else:
        raise NotImplementedError(
            f"Strategy '{strategy}' is defined in catalog but not implemented in loader."
        )

    # 5. Optional: Hybrid Public Sector Injection
    if include_public_sector:
        logger.info("    🧩 Injecting Public Sector data (Schools + Health)...")
        
        # A. Fetch Schools (INEP)
        try:
            from atlasbr.app.inep import load_schools
            schools = load_schools(
                places=places,
                year=year, # Assuming matching year exists
                gcp_billing=project_id,
                as_gdf=False
            )
        except Exception as e:
            logger.warning(f"Failed to load Schools for {year}: {e}. Skipping injection.")
            schools = pd.DataFrame()

        # B. Fetch Health (CNES)
        try:
            from atlasbr.app.cnes import load_cnes
            # CNES is monthly; we typically use September (09) as the reference
            health = load_cnes(
                places=places,
                year=year,
                month=9,
                gcp_billing=project_id, 
                geocode=False
            )
        except Exception as e:
            logger.warning(f"Failed to load CNES for {year}: {e}. Skipping injection.")
            health = pd.DataFrame()
        
        # C. Harmonize
        # These functions align columns to RAIS standards (cnae_2, etc.)
        schools_h = integration.harmonize_schools_to_rais(schools)
        health_h = integration.harmonize_cnes_to_rais(health)
        
        # Standardize ID column names for the merge
        if not schools_h.empty and "id_escola" in schools_h.columns:
             schools_h = schools_h.rename(columns={"id_escola": "id_estab_original"})
        
        if not health_h.empty and "id_estabelecimento_cnes" in health_h.columns:
             health_h = health_h.rename(columns={"id_estabelecimento_cnes": "id_estab_original"})

        # D. Hybrid Merge
        to_merge = [main_dataset]
        if not schools_h.empty: to_merge.append(schools_h)
        if not health_h.empty: to_merge.append(health_h)
        
        if len(to_merge) > 1:
            main_dataset = pd.concat(to_merge, ignore_index=True)
            logger.info(
                f"       -> Integrated {len(schools_h)} schools and {len(health_h)} health units."
            )

    # 6. Optional: Geocoding
    if geocode:
        from atlasbr.infra.adapters import ceps_bd
        
        df_ceps = ceps_bd.fetch_ceps_from_bd(
            munis=muni_ids,
            billing_id=resolve_billing_id(gcp_billing)
        )
        
        logger.info(f"    🌍 Geocoding {len(main_dataset)} establishments via CEP...")
        gdf_rais = geocoding.geocode_by_cep(
            data_df=main_dataset,
            cep_df=df_ceps,
            data_cep_col="cep"
        )
        logger.info(f"✅ Loaded {len(gdf_rais)} establishments (Geolocated).")
        return gdf_rais
    
    logger.info(f"✅ Loaded {len(main_dataset)} establishments (Tabular).")
    return main_dataset