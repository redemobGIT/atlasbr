"""
AtlasBR - Core Logic for Census Data.

This module contains pure functions to harmonize and transform raw Census data.
It handles column renaming, summation of age groups, and imputation of missing data.
"""

import pandas as pd
import numpy as np
from typing import List

# --- Constants ---

CENSO_RACES = ("branca", "preta", "amarela", "parda", "indigena")

# --- Helpers ---

def _sum_cols(df: pd.DataFrame, pattern: str = "v", start: int = 0, end: int = 0, width: int = 3) -> pd.Series:
    """
    Sums a range of columns (e.g., v035 to v048).
    Handles missing columns gracefully by ignoring them (assuming 0).
    """
    cols = [f"{pattern}{i:0{width}d}" for i in range(start, end)]
    # Only select columns that actually exist in the fetched dataframe
    valid_cols = [c for c in cols if c in df.columns]
    
    if not valid_cols:
        return pd.Series(0, index=df.index)
    
    return df[valid_cols].sum(axis=1)

# --- 2010 Transformers ---

def harmonize_basic_2010(df: pd.DataFrame) -> pd.DataFrame:
    """Renames basic variables to canonical names."""
    return df.rename(columns={
        "v002": "habitantes", 
        "v001": "domicilios"
    })

def harmonize_income_2010(df: pd.DataFrame) -> pd.DataFrame:
    """Renames income variables."""
    return df.rename(columns={"v009": "rendimento_medio"})

def process_age_2010(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates raw 2010 age columns into standard brackets.
    Logic derived from original _age_2010 SQL.
    """
    df = df.fillna(0)
    
    # Calculate brackets
    # 0-14: v022 (Total?) + range(35, 49)
    # Note: Validate if v022 is needed or if it was specific to the SQL logic.
    # Assuming v022 is 'Total under 1 year' or similar start of the block.
    df["age_0_14"] = df.get("v022", 0) + _sum_cols(df, "v", 35, 49, width=3)
    
    df["age_15_19"] = _sum_cols(df, "v", 49, 54, width=3)
    df["age_20_64"] = _sum_cols(df, "v", 54, 99, width=3)
    df["age_65p"]   = _sum_cols(df, "v", 99, 135, width=3)
    
    return df[["age_0_14", "age_15_19", "age_20_64", "age_65p"]]

def process_race_2010(df: pd.DataFrame) -> pd.DataFrame:
    """Renames race columns for 2010."""
    mapping = {
        "v002": "race_branca",
        "v003": "race_preta",
        "v004": "race_amarela",
        "v005": "race_parda",
        "v006": "race_indigena"
    }
    return df.rename(columns=mapping)[list(mapping.values())]

# --- 2022 Transformers ---

def process_age_2022(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates raw 2022 age columns.
    Logic: Total - (Adults + Elderly) = Children (Residual calculation).
    """
    df = df.fillna(0)
    
    # 15-19 is V00644
    df["age_15_19"] = df.get("V00644", 0)
    
    # 20-64 is range 645 to 654 (exclusive of 654? check range logic)
    # Original logic: range(645, 654) -> V645...V653
    df["age_20_64"] = _sum_cols(df, "V", 645, 654, width=5)
    
    # 65+ is range(654, 657) -> V654, V655, V656
    df["age_65p"] = _sum_cols(df, "V", 654, 657, width=5)
    
    # 0-14 is the residual
    df["age_0_14"] = df["pessoas"] - (df["age_15_19"] + df["age_20_64"] + df["age_65p"])
    
    # Guard against negative numbers due to data errors
    df["age_0_14"] = df["age_0_14"].clip(lower=0)
    
    return df[["age_0_14", "age_15_19", "age_20_64", "age_65p"]]

def process_race_2022(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes Race data for 2022.
    Imputes race for children (0-14) based on the distribution of adults (15+) 
    in the same municipality.
    """
    df = df.fillna(0).copy()
    
    # 1. Calculate Total Population 15+
    # Range 644 to 657 covers all age groups 15+
    df["pop_15p"] = _sum_cols(df, "V", 644, 657, width=5)
    
    # 2. Calculate Total Children (0-14) as residual
    df["age_0_14"] = df["pessoas"] - df["pop_15p"]
    df["age_0_14"] = df["age_0_14"].clip(lower=0)
    
    # 3. Calculate 15+ totals for each race
    # Columns are interleaved: 
    # V00657 (Branca Group 1), V00658 (Preta Group 1)... 
    # V00662 (Branca Group 2), V00663 (Preta Group 2)...
    for i, race in enumerate(CENSO_RACES):
        # Start at 657 + offset(0..4), step 5
        cols = [f"V{c:05d}" for c in range(657 + i, 717, 5)]
        # Filter strictly existing columns
        valid_cols = [c for c in cols if c in df.columns]
        df[f"race_{race}_15p"] = df[valid_cols].sum(axis=1)

    # 4. Imputation Strategy
    # We need the municipality code. Assuming index is 'id_setor_censitario'.
    # Extract first 7 digits.
    if "id_mun" not in df.columns:
        # If index is named, use it; else fallback to column 'id_setor_censitario'
        idx = df.index.to_series() if df.index.name == "id_setor_censitario" else df["id_setor_censitario"]
        df["id_mun"] = idx.astype(str).str.slice(0, 7)

    # Group by Municipality to get the racial distribution of adults
    race_cols_15p = [f"race_{r}_15p" for r in CENSO_RACES]
    
    # Sum people by muni
    muni_sums = df.groupby("id_mun")[race_cols_15p + ["pop_15p"]].sum()
    
    # Calculate ratios (avoid division by zero)
    # Result: DataFrame of ratios indexed by id_mun
    with np.errstate(divide='ignore', invalid='ignore'):
        muni_ratios = muni_sums[race_cols_15p].div(muni_sums["pop_15p"], axis=0).fillna(0)

    # 5. Apply ratios to children
    for race in CENSO_RACES:
        col_15p = f"race_{race}_15p"
        
        # Map the municipality ratio back to the sector rows
        ratio_series = df["id_mun"].map(muni_ratios[col_15p])
        
        # Total Race = (Adults of Race X) + (Children * Ratio of Race X)
        df[f"race_{race}"] = df[col_15p] + (df["age_0_14"] * ratio_series)

    return df[[f"race_{r}" for r in CENSO_RACES]]