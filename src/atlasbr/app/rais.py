"""
AtlasBR - Application Layer for RAIS (Hybrid Pipeline).
"""
import pandas as pd
import geopandas as gpd
from typing import List, Union

from atlasbr.core.catalog.rais import get_rais_spec
from atlasbr.core.logic import rais as logic, geocoding, integration
from atlasbr.infra.adapters import rais_bd, ceps_bd
from atlasbr.infra.geo import resolver
from atlasbr.settings import logger
from atlasbr.core.types import PlaceInput

# Sub-apps
from atlasbr.app.inep import load_schools
from atlasbr.app.cnes import load_cnes

def load_rais(
    places: List[PlaceInput],
    *,
    year: int = 2022,
    gcp_billing: str,
    geocode: bool = False,
    include_public_sector: bool = False,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    
    # 1. Resolve & Fetch RAIS
    muni_ids = resolver.resolve_places_to_ids(places)
    spec = get_rais_spec(year)
    df_rais = rais_bd.fetch_rais_from_bd(
        spec.table_id, spec.required_columns, muni_ids, year, gcp_billing
    )
    
    # 2. Logic & Cleaning
    df_rais = logic.filter_invalid_legal_nature(df_rais)
    df_rais = logic.clip_outlier_jobs(df_rais)
    
    # 3. Geocode RAIS (Stream 1)
    if geocode:
        logger.info(f"    🌍 Geocoding RAIS via CEP...")
        df_ceps = ceps_bd.fetch_ceps_from_bd(muni_ids, gcp_billing)
        main_dataset = geocoding.geocode_by_cep(df_rais, df_ceps, "cep")
        main_dataset = main_dataset.rename(columns={"id_estabelecimento": "id_estab_original"})
    else:
        main_dataset = df_rais
        main_dataset["id_estab_original"] = None

    # 4. Inject Public Sector (Stream 2 & 3)
    if include_public_sector:
        logger.info("    ➕ Injecting Public Sector (Schools & Health)...")
        
        # A. Load Schools (Lat/Lon)
        schools = load_schools(muni_ids, year=2023, gcp_billing=gcp_billing, as_gdf=geocode)
        
        # B. Load CNES (CEP)
        health = load_cnes(muni_ids, year=2023, month=9, gcp_billing=gcp_billing, geocode=geocode)
        
        # C. Harmonize
        schools_h = integration.harmonize_schools_to_rais(schools)
        health_h = integration.harmonize_cnes_to_rais(health)
        
        # D. Hybrid Merge
        main_dataset = pd.concat([main_dataset, schools_h, health_h], ignore_index=True)
        logger.info(f"       -> Integrated {len(schools_h)} schools and {len(health_h)} health units.")

    # 5. Enrich Metadata
    main_dataset = logic.enrich_cnae_metadata(main_dataset, cnae_col="cnae_2")
    
    logger.info(f"✅ Loaded {len(main_dataset)} total establishments.")
    return main_dataset