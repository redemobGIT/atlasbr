from __future__ import annotations

from typing import Iterable, Optional, Sequence, Tuple

import pandas as pd


def require_columns(df: pd.DataFrame, cols: Sequence[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")


def coerce_numeric(s: pd.Series, fillna: Optional[float] = None) -> pd.Series:
    out = pd.to_numeric(s, errors="coerce")
    if fillna is not None:
        out = out.fillna(fillna)
    return out


def top_k_categories(
    s: pd.Series,
    *,
    top_k: Optional[int],
    other_label: str = "Other",
) -> Tuple[pd.Series, list]:
    if top_k is None:
        cats = sorted([c for c in s.dropna().unique().tolist()])
        return s, cats

    counts = s.value_counts(dropna=True)
    keep = counts.head(int(top_k)).index.tolist()
    mapped = s.where(s.isin(keep), other_label)
    cats = keep + ([other_label] if (mapped == other_label).any() else [])
    return mapped, cats


def iter_groups(
    df: pd.DataFrame,
    hue: str,
    cats: Iterable,
) -> Iterable[Tuple[str, pd.DataFrame]]:
    for c in cats:
        sub = df[df[hue] == c]
        yield str(c), sub
