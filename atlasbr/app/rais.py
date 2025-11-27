"""
AtlasBR - Application Layer for RAIS.
"""

import pandas as pd
import geopandas as gpd
from typing import List, Union, Tuple

# Internal Modules
from atlasbr.core.catalog.rais import get_rais_spec
from atlasbr.core.logic import rais as logic
from atlasbr.core.logic import geocoding, integration
from atlasbr.infra.adapters import rais_bd, ceps_bd
from atlasbr.geo import resolver

# --- The Federation: Import other Apps ---
from atlasbr.app.schools import load_schools
from atlasbr.app.cnes import load_cnes

PlaceInput = Union[int, str, Tuple[str, str]]

def load_rais(
    places: List[PlaceInput],
    *,
    year: int = 2022,
    gcp_billing: str,
    geocode: bool = False,
    include_public_sector: bool = False,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Loads RAIS data, optionally enriching it with Public Sector data (Schools/CNES).
    """
    
    # 1. Resolve Inputs
    muni_ids = resolver.resolve_places(places)
    
    # 2. Fetch & Clean RAIS (The Core Data)
    spec = get_rais_spec(year)
    df_rais = rais_bd.fetch_rais_from_bd(
        spec.table_id, spec.required_columns, muni_ids, year, gcp_billing
    )
    
    df_rais = logic.filter_invalid_legal_nature(df_rais)
    df_rais = logic.clip_outlier_jobs(df_rais)
    
    # 3. Geocode RAIS (If requested)
    # We do this *before* merging because RAIS uses CEPs, but Schools use Lat/Lon.
    if geocode:
        print(f"    🌍 Geocoding RAIS via CEP...")
        df_ceps = ceps_bd.fetch_ceps_from_bd(muni_ids, gcp_billing)
        main_dataset = geocoding.geocode_by_cep(df_rais, df_ceps, "cep")
        # Rename id for consistency with the harmonized set
        main_dataset = main_dataset.rename(columns={"id_estabelecimento": "id_estab_original"}) 
    else:
        main_dataset = df_rais
        main_dataset["id_estab_original"] = None # Placeholder if original ID not in raw fetch

    # 4. Inject Public Sector (The Federation Step)
    if include_public_sector:
        print("    ➕ Injecting Public Sector (calling schools & cnes)...")
        
        # A. Call Schools App
        # We reuse the logic we already wrote! 
        # If geocode=True, it returns a GDF (Points). If False, DF.
        schools_data = load_schools(
            places=muni_ids, 
            year=2023, 
            gcp_billing=gcp_billing, 
            as_gdf=geocode
        )
        
        # B. Call CNES App
        # Reusing CNES logic.
        cnes_data = load_cnes(
            places=muni_ids, 
            year=2023, month=9, 
            gcp_billing=gcp_billing, 
            geocode=geocode
        )
        
        # C. Harmonize
        # This aligns columns AND assigns the proper CNAE codes
        schools_h = integration.harmonize_schools_to_rais(schools_data)
        health_h = integration.harmonize_cnes_to_rais(cnes_data)
        
        # D. Merge
        # pd.concat is smart enough to handle Mixed DFs and GDFs
        # (It returns a GDF if at least one input is a GDF and geometry col aligns)
        main_dataset = pd.concat([main_dataset, schools_h, health_h], ignore_index=True)
        
        print(f"       -> Integrated {len(schools_h)} schools (Section P) and {len(health_h)} health units (Section Q).")

    # 5. Enrich Metadata
    # This runs on the WHOLE dataset.
    # - Real RAIS rows get their Sector Name from their real CNAE.
    # - School rows get "Educação" from the injected "8513900".
    # - Health rows get "Saúde..." from the injected "8610101".
    main_dataset = logic.enrich_cnae_metadata(main_dataset, cnae_col="cnae_2")
    
    print(f"✅ Loaded {len(main_dataset)} total establishments.")
    return main_dataset