"""
AtlasBR - Core Logic for RAIS Data.

Handles CNAE classification and statistical outlier removal for job counts.
"""

import pandas as pd
import numpy as np
from atlasbr.core.catalog.rais import (
    CNAE_SECTIONS_DEF, CNAE_SECTOR_NAMES, CNAE_PROBLEM_PREFIXES
)

# Pre-compute the Prefix Map for O(1) lookups
_CNAE_PREFIX_TO_SECTION = {
    f"{i:02d}": letter
    for letter, start, end in CNAE_SECTIONS_DEF
    for i in range(start, end + 1)
}

def enrich_cnae_metadata(df: pd.DataFrame, cnae_col: str = "cnae_2") -> pd.DataFrame:
    """Adds 'section_letter' and 'sector_name' columns based on CNAE code."""
    if df.empty:
        return df
        
    # Extract first 2 digits
    prefixes = df[cnae_col].astype(str).str.zfill(7).str[:2]
    
    df["cnae_section"] = prefixes.map(_CNAE_PREFIX_TO_SECTION)
    df["cnae_sector"] = df["cnae_section"].map(CNAE_SECTOR_NAMES)
    
    return df

def clip_outlier_jobs(df: pd.DataFrame, jobs_col: str = "quantidade_vinculos_ativos") -> pd.DataFrame:
    """
    Clips job counts for problematic sectors (e.g., HQ of cleaning companies).
    Logic ported from original SQL:
    If CNAE prefix is problematic AND jobs > p95 of that prefix -> Clip to p95.
    """
    df = df.copy()
    
    # 1. Identify Problematic Rows
    # Check if CNAE starts with any problem prefix
    # Tuple inside startswith is faster than regex in pure Python/Pandas loops usually
    # But for vectorized pandas, str.startswith(tuple) works great
    is_problematic = df["cnae_2"].astype(str).str.startswith(tuple(CNAE_PROBLEM_PREFIXES))
    
    if not is_problematic.any():
        return df

    # 2. Calculate P95 per Prefix (only for the problematic subset to save time)
    # Extract prefix for grouping
    df["_prefix"] = df["cnae_2"].astype(str).str.extract(r'^(\d{2,3})')[0]
    
    # Calculate thresholds
    # We only care about stats for rows that are potentially problematic
    stats = df[is_problematic].groupby("_prefix")[jobs_col].quantile(0.95)
    
    # 3. Apply Clipping
    # Map p95 back to the main dataframe
    df["_p95"] = df["_prefix"].map(stats)
    
    # Logic: If problem AND jobs > p95, use p95, else use original
    mask_clip = is_problematic & (df[jobs_col] > df["_p95"])
    
    # Assign clipped values (cast to int if needed, though NaNs make it float)
    df.loc[mask_clip, jobs_col] = df.loc[mask_clip, "_p95"]
    
    # Cleanup helper columns
    return df.drop(columns=["_prefix", "_p95"])

def filter_invalid_legal_nature(df: pd.DataFrame) -> pd.DataFrame:
    """Removes Public Administration entities (starts with '1') or '2011'."""
    # Logic from original SQL: NOT (LEFT(natureza, 1)='1' OR nat='2011')
    nat = df["natureza_juridica"].astype(str)
    mask_valid = ~(nat.str.startswith("1") | (nat == "2011"))
    return df[mask_valid]