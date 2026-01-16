"""
AtlasBR - Core Logic for RAIS Data.

Handles CNAE classification and statistical outlier removal for job counts.
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from atlasbr.core.catalog.rais import (
    CNAE_PROBLEM_PREFIXES,
    CNAE_SECTIONS_DEF,
    CNAE_SECTOR_NAMES,
)

_CNAE_PREFIX_TO_SECTION = {
    f"{i:02d}": letter
    for letter, start, end in CNAE_SECTIONS_DEF
    for i in range(start, end + 1)
}

_PROB2 = {p for p in CNAE_PROBLEM_PREFIXES if len(p) == 2}
_PROB3 = {p for p in CNAE_PROBLEM_PREFIXES if len(p) == 3}


def enrich_cnae_metadata(
    df: pd.DataFrame,
    cnae_col: str = "cnae_2",
) -> pd.DataFrame:
    if df.empty:
        return df
    if cnae_col not in df.columns:
        raise ValueError(f"Missing column: {cnae_col!r}")

    out = df.copy()
    digits = out[cnae_col].astype(str).str.replace(r"\D", "", regex=True)
    prefixes = digits.str.extract(r"^(\d{2})", expand=False)

    out["cnae_section"] = prefixes.map(_CNAE_PREFIX_TO_SECTION)
    out["cnae_sector"] = out["cnae_section"].map(CNAE_SECTOR_NAMES)
    return out


def clip_outlier_jobs(
    df: pd.DataFrame,
    jobs_col: str = "quantidade_vinculos_ativos",
    cnae_col: str = "cnae_2",
) -> pd.DataFrame:
    if df.empty:
        return df
    if cnae_col not in df.columns:
        raise ValueError(f"Missing column: {cnae_col!r}")
    if jobs_col not in df.columns:
        raise ValueError(f"Missing column: {jobs_col!r}")

    out = df.copy()
    digits = out[cnae_col].astype(str).str.replace(r"\D", "", regex=True)
    p2 = digits.str[:2]
    p3 = digits.str[:3]

    is_problem = p2.isin(_PROB2) | p3.isin(_PROB3)
    if not is_problem.any():
        return out

    prefix = p3.where(p3.isin(_PROB3), p2)
    out["_prefix"] = prefix
    out["_jobs"] = pd.to_numeric(out[jobs_col], errors="coerce")

    p95 = out.loc[is_problem].groupby("_prefix")["_jobs"].quantile(0.95)
    out["_p95"] = out["_prefix"].map(p95)

    mask = is_problem & (out["_jobs"] > out["_p95"])
    if mask.any():
        clipped = out.loc[mask, "_p95"].fillna(0).round().astype(int)
        out.loc[mask, jobs_col] = clipped

    return out.drop(columns=["_prefix", "_jobs", "_p95"])


def filter_invalid_legal_nature(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "natureza_juridica" not in df.columns:
        raise ValueError("Missing column: 'natureza_juridica'")

    nat = df["natureza_juridica"].astype(str)
    mask_valid = ~(nat.str.startswith("1") | (nat == "2011"))
    return df.loc[mask_valid]
