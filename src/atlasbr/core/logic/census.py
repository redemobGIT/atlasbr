"""
AtlasBR - Core Logic for Census Data.

This module contains pure functions to harmonize and transform raw Census data.
It handles column renaming, summation of age groups, and imputation of missing.
It is strategy-aware: handles raw BigQuery columns differently from FTP CSVs.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Callable, Any

# --- Constants ---

CENSO_RACES = ("branca", "preta", "amarela", "parda", "indigena")

# --- Helpers ---


def _sum_cols(
    df: pd.DataFrame,
    pattern: str = "v",
    start: int = 0,
    end: int = 0,
    width: int = 3,
    step: int = 1
) -> pd.Series:
    """
    Sums a range of columns (e.g., v035 to v048).
    Handles missing columns gracefully by ignoring them (assuming 0).
    """
    cols = [f"{pattern}{i:0{width}d}" for i in range(start, end, step)]
    # Only select columns that actually exist in the fetched dataframe
    valid_cols = [c for c in cols if c in df.columns]

    if not valid_cols:
        return pd.Series(0, index=df.index)

    return df[valid_cols].sum(axis=1)

# --- 2010 Transformers ---


def _handle_basic_2010(df: pd.DataFrame, strategy: str) -> pd.DataFrame:
    """Renames basic variables to canonical names."""
    return df.rename(columns={
        "v002": "habitantes",
        "v001": "domicilios"
    })


def _handle_income_2010(df: pd.DataFrame, strategy: str) -> pd.DataFrame:
    """Renames income variables."""
    return df.rename(columns={"v009": "rendimento_medio"})


def _handle_age_2010(df: pd.DataFrame, strategy: str) -> pd.DataFrame:
    """
    Aggregates raw 2010 age columns into standard brackets.
    """
    df = df.fillna(0)

    # Calculate brackets
    # 0-14: v022 (Total <1y) + range(35, 49)
    df["age_0_14"] = df.get("v022", 0) + _sum_cols(df, "v", 35, 49, width=3)

    df["age_15_19"] = _sum_cols(df, "v", 49, 54, width=3)
    df["age_20_64"] = _sum_cols(df, "v", 54, 99, width=3)
    df["age_65p"] = _sum_cols(df, "v", 99, 135, width=3)

    return df[["age_0_14", "age_15_19", "age_20_64", "age_65p"]]


def _handle_race_2010(df: pd.DataFrame, strategy: str) -> pd.DataFrame:
    """Renames race columns for 2010."""
    mapping = {
        "v002": "cor_branca",
        "v003": "cor_preta",
        "v004": "cor_amarela",
        "v005": "cor_parda",
        "v006": "cor_indigena"
    }
    df = df.rename(columns=mapping)
    # Return only the canonical columns if they exist
    cols = [c for c in mapping.values() if c in df.columns]
    return df[cols]

# --- 2022 Transformers ---


def _handle_basic_2022(df: pd.DataFrame, strategy: str) -> pd.DataFrame:
    if strategy == "bd_table":
        return df.rename(columns={"pessoas": "habitantes"})
    # FTP already mapped via Catalog
    return df


def _handle_income_2022(df: pd.DataFrame, strategy: str) -> pd.DataFrame:
    # FTP already mapped to 'rendimento_medio'
    return df


def _handle_age_2022(df: pd.DataFrame, strategy: str) -> pd.DataFrame:
    """
    Aggregates raw 2022 age columns.
    Handles 'bd_table' (Preliminar) and 'ftp_csv' (Alfabetizacao) schemas.
    """
    # 1. Numeric coercion (Crucial for FTP "X" values)
    # Apply to all 'V' columns to ensure summation works
    for col in [c for c in df.columns if c.startswith("V")]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 2. Strategy Branching
    if strategy == "bd_table":
        # Base dos Dados (Preliminar Table)
        # V00644 (15-19), V00645..654 (20-64), V00654..657 (65+)
        df["age_15_19"] = df.get("V00644", 0)
        df["age_20_64"] = _sum_cols(df, "V", 645, 654, width=5)
        df["age_65p"] = _sum_cols(df, "V", 654, 657, width=5)
        total_col = "pessoas"

    elif strategy == "ftp_csv":
        # FTP (Alfabetizacao Table)
        # V00644 (15-19)
        # V00649..674 (20-64, step 5)
        # V00679 (65+)
        df["age_15_19"] = df.get("V00644", 0)
        df["age_20_64"] = _sum_cols(df, "V", 649, 675, width=5, step=5)
        df["age_65p"] = df.get("V00679", 0)
        # FTP payloads are thematic; total pop might not be in this file
        total_col = "habitantes" if "habitantes" in df.columns else None
    else:
        return df

    # 3. Residual Logic for 0-14
    # Only calculate if we explicitly have a total population column.
    if total_col and total_col in df.columns:
        adults_elderly = (
            df["age_15_19"] + df["age_20_64"] + df["age_65p"]
        )
        df["age_0_14"] = (df[total_col] - adults_elderly).clip(lower=0)
    else:
        # App layer must derive this after merging with Basic theme
        df["age_0_14"] = np.nan

    return df[["age_0_14", "age_15_19", "age_20_64", "age_65p"]]


def _handle_race_2022(df: pd.DataFrame, strategy: str) -> pd.DataFrame:
    """
    Processes Race data for 2022.
    """
    # PATH A: FTP Strategy (Simple Aggregates)
    if strategy == "ftp_csv":
        expected_cols = [f"cor_{r}" for r in CENSO_RACES]
        return df[[c for c in expected_cols if c in df.columns]]

    # PATH B: BigQuery Strategy (Complex Imputation)
    # Imputes race for children (0-14) based on adult distribution.
    df = df.fillna(0).copy()

    # 1. Calculate Total Population 15+
    df["pop_15p"] = _sum_cols(df, "V", 644, 657, width=5)

    # 2. Calculate Total Children (0-14) as residual
    if "pessoas" not in df.columns:
        return df

    df["age_0_14"] = (df["pessoas"] - df["pop_15p"]).clip(lower=0)

    # 3. Calculate 15+ totals for each race
    for i, race in enumerate(CENSO_RACES):
        # Start at 657 + offset(0..4), step 5
        cols = [f"V{c:05d}" for c in range(657 + i, 717, 5)]
        valid_cols = [c for c in cols if c in df.columns]
        df[f"race_{race}_15p"] = df[valid_cols].sum(axis=1)

    # 4. Imputation Strategy
    if "id_mun" not in df.columns:
        # Infer muni code from index if possible
        idx = (
            df.index.to_series()
            if df.index.name == "id_setor_censitario"
            else df.get("id_setor_censitario", pd.Series())
        )
        if not idx.empty:
            df["id_mun"] = idx.astype(str).str.slice(0, 7)

    if "id_mun" in df.columns:
        race_cols_15p = [f"race_{r}_15p" for r in CENSO_RACES]
        muni_sums = df.groupby("id_mun")[race_cols_15p + ["pop_15p"]].sum()

        with np.errstate(divide='ignore', invalid='ignore'):
            muni_ratios = muni_sums[race_cols_15p].div(
                muni_sums["pop_15p"], axis=0
            ).fillna(0)

        # 5. Apply ratios
        for race in CENSO_RACES:
            col_15p = f"race_{race}_15p"
            ratio_series = df["id_mun"].map(muni_ratios[col_15p])

            target_col = f"cor_{race}"
            df[target_col] = df[col_15p] + (df["age_0_14"] * ratio_series)

    return df[[f"cor_{r}" for r in CENSO_RACES if f"cor_{r}" in df.columns]]


# --- Dispatcher ---

_HANDLERS: Dict[tuple, Callable[[pd.DataFrame, str], pd.DataFrame]] = {
    ("basic", 2010): _handle_basic_2010,
    ("basic", 2022): _handle_basic_2022,
    ("income", 2010): _handle_income_2010,
    ("income", 2022): _handle_income_2022,
    ("race", 2010): _handle_race_2010,
    ("race", 2022): _handle_race_2022,
    ("age", 2010): _handle_age_2010,
    ("age", 2022): _handle_age_2022,
}


def standardize_census_dataframe(
    df: pd.DataFrame,
    theme: str,
    year: int,
    strategy: str
) -> pd.DataFrame:
    """
    Main dispatch function to harmonize raw Census dataframes.
    """
    handler = _HANDLERS.get((theme, year))
    if handler:
        return handler(df, strategy)
    return df


def apply_census_logic(df: pd.DataFrame, spec: Any) -> pd.DataFrame:
    """
    Public Facade: Applies the correct transformation based on the Spec.
    """
    return standardize_census_dataframe(
        df,
        theme=spec.theme,
        year=spec.year,
        strategy=spec.strategy
    )