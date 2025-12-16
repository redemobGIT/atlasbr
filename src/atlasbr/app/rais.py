"""
AtlasBR - Application Layer for RAIS (Hybrid Pipeline).
"""
import pandas as pd
import geopandas as gpd
import numpy as np
from typing import List, Union, Optional

from atlasbr.core.catalog.rais import get_rais_spec
from atlasbr.core.logic import rais as logic, geocoding, integration
from atlasbr.infra.geo import resolver
from atlasbr.settings import logger, resolve_billing_id
from atlasbr.core.types import PlaceInput

def load_rais(
    places: List[PlaceInput],
    *,
    year: int = 2022,
    gcp_billing: Optional[str] = None,
    strategy: str,
    geocode: bool = False,
    include_public_sector: bool = False,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    
    # 1. Configuration
    project_id = resolve_billing_id(gcp_billing)

    # 2. Resolve & Fetch RAIS
    muni_ids = resolver.resolve_places_to_ids(places)
    spec = get_rais_spec(year)
    
    if spec.strategy == "bd_table":
        from atlasbr.infra.adapters import rais_bd
        df_rais = rais_bd.fetch_rais_from_bd(
            spec.table_id, spec.required_columns, muni_ids, year, project_id
        )
    else:
        raise NotImplementedError(f"Strategy {strategy} not implemented for RAIS")

    # 3. Canonical Identity & Cleaning
    # RAIS rows are unique establishments, but lack a public ID column.
    # We create a stable surrogate key for internal tracking.
    df_rais["id_estab_original"] = (
        f"RAIS_{year}_" + df_rais.reset_index().index.astype(str)
    )
    
    df_rais = logic.filter_invalid_legal_nature(df_rais)
    df_rais = logic.clip_outlier_jobs(df_rais)
    
    # 4. Geocode RAIS (Stream 1)
    if geocode:
        logger.info(f"    🌍 Geocoding RAIS via CEP...")
        from atlasbr.infra.adapters import ceps_bd
        df_ceps = ceps_bd.fetch_ceps_from_bd(muni_ids, project_id)
        
        # Merge geometries onto the main dataset
        main_dataset = geocoding.geocode_by_cep(df_rais, df_ceps, "cep")
    else:
        main_dataset = df_rais

    # 5. Inject Public Sector (Stream 2 & 3)
    if include_public_sector:
        logger.info(f"    ➕ Injecting Public Sector (Schools & Health) for {year}...")
        
        from atlasbr.app.inep import load_schools
        from atlasbr.app.cnes import load_cnes

        # A. Load Schools (Lat/Lon)
        try:
            schools = load_schools(
                places=muni_ids,
                year=year,
                gcp_billing=project_id, 
                as_gdf=geocode
            )
        except Exception as e:
            logger.warning(f"Failed to load Schools for {year}: {e}. Skipping injection.")
            schools = pd.DataFrame()

        # B. Load CNES (CEP)
        try:
            health = load_cnes(
                places=muni_ids, 
                year=year,
                month=9,
                gcp_billing=project_id, 
                geocode=geocode
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
            logger.info(f"       -> Integrated {len(schools_h)} schools and {len(health_h)} health units.")

    # 6. Enrich Metadata
    main_dataset = logic.enrich_cnae_metadata(main_dataset, cnae_col="cnae_2")
    
    logger.info(f"✅ Loaded {len(main_dataset)} total establishments.")
    
    return main_dataset