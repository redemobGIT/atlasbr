"""
AtlasBR - Integration Logic.

Adapts Schools and CNES dataframes to match the RAIS schema,
including assigning proxy CNAE codes and column mapping.
"""

import pandas as pd
import geopandas as gpd
from atlasbr.core.logic import rais as rais_logic

# --- Constants: Proxy CNAEs ---
# We use specific 7-digit codes so the enricher can find the Section/Sector.

# 85.13-9-00: Ensino fundamental
# Maps to Section P (Educação)
CNAE_DEFAULT_EDUCATION = "8513900" 

# 86.10-1-01: Atividades de atendimento hospitalar
# Maps to Section Q (Saúde humana e serviços sociais)
CNAE_DEFAULT_HEALTH = "8610101" 

def _harmonize_generic(
    df: pd.DataFrame, 
    rename_map: dict, 
    cnae_code: str, 
    estab_type: str
) -> pd.DataFrame:
    """
    Shared logic to map columns, assign CNAEs, and enrich metadata.
    """
    df = df.copy()
    
    # 1. Rename to match RAIS columns (e.g. 'quantidade_profissional' -> 'quantidade_vinculos_ativos')
    df = df.rename(columns=rename_map)
    
    # 2. Assign RAIS-compatible static values
    df["cnae_2"] = cnae_code
    df["natureza_juridica"] = "1000"  # Public Administration Generic
    df["tipo_estabelecimento"] = estab_type
    
    # 3. Handle 'cep'
    # Schools might not have it (None), CNES/RAIS do. Ensure it exists for consistency.
    if "cep" not in df.columns:
        df["cep"] = None

    # 4. Enrich with CNAE Metadata (Section Letter, Sector Name)
    # This ensures these rows look EXACTLY like native RAIS rows
    df = rais_logic.enrich_cnae_metadata(df, cnae_col="cnae_2")
    
    # 5. Select Output Columns
    # We define the strict schema of RAIS to ensure clean concatenation
    cols = [
        "id_estab_original", 
        "tipo_estabelecimento", 
        "cnae_2", 
        "cnae_section",       # Added by enricher
        "cnae_sector",        # Added by enricher
        "quantidade_vinculos_ativos", 
        "cep", 
        "natureza_juridica"
    ]
    
    # Preserve Geometry if it exists (for geocoded pipelines)
    if isinstance(df, gpd.GeoDataFrame) or "geometry" in df.columns:
        cols.append("geometry")
        
    # Return only columns that exist (in case enricher failed or geometry missing)
    final_cols = [c for c in cols if c in df.columns]
    
    return df[final_cols]

def harmonize_schools_to_rais(df_schools: pd.DataFrame) -> pd.DataFrame:
    """Adapts School data to RAIS schema."""
    # Filter only Public schools if 'rede' column exists
    if "rede" in df_schools.columns:
        df = df_schools[df_schools["rede"] == "Publica"]
    else:
        df = df_schools

    return _harmonize_generic(
        df,
        rename_map={
            "quantidade_profissional": "quantidade_vinculos_ativos", 
            "id_escola": "id_estab_original"
        },
        cnae_code=CNAE_DEFAULT_EDUCATION,
        estab_type="Escola (INEP)"
    )

def harmonize_cnes_to_rais(df_cnes: pd.DataFrame) -> pd.DataFrame:
    """Adapts CNES data to RAIS schema."""
    return _harmonize_generic(
        df_cnes,
        rename_map={
            "quantidade_trabalhadores_saude": "quantidade_vinculos_ativos", 
            "id_estabelecimento_cnes": "id_estab_original"
        },
        cnae_code=CNAE_DEFAULT_HEALTH,
        estab_type="Saude (CNES)"
    )