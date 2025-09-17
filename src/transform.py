from __future__ import annotations
from typing import Iterable
import numpy as np
import pandas as pd

def aggregate(df: pd.DataFrame, *, by: list[str], specs: dict[str, str | tuple]) -> pd.DataFrame:
    grouped = df.groupby(by, dropna=False)
    return grouped.agg(specs).reset_index()

def winsorize(df: pd.DataFrame, cols: Iterable[str], *, method: str = "iqr", k: float = 1.5) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        s = out[col].astype(float)
        if method == "iqr":
            q1, q3 = s.quantile([0.25, 0.75])
            iqr = q3 - q1
            lo, hi = q1 - k * iqr, q3 + k * iqr
        elif method == "mad":
            med = s.median()
            mad = (s - med).abs().median()
            lo, hi = med - k * mad, med + k * mad
        else:
            raise ValueError("method deve ser 'iqr' ou 'mad'")
        out[col] = s.clip(lo, hi)
    return out

def empirical_bayes(counts: pd.Series, prior: float | None = None) -> pd.Series:
    c = counts.astype(float)
    mu = float(c.mean()) if prior is None else float(prior)
    var = float(c.var(ddof=1)) if len(c) > 1 else 0.0
    if var == 0.0:
        return pd.Series(np.full_like(c, mu), index=c.index, dtype=float)
    w = var / (var + (c + 1.0))
    return w * mu + (1 - w) * c

def build_demographics(df: pd.DataFrame, *, bins_config: dict[str, list[int]]) -> pd.DataFrame:
    out = df.copy()
    for col, edges in bins_config.items():
        labels = [f"{edges[i]}_{edges[i+1]-1}" for i in range(len(edges) - 1)]
        cat = pd.cut(out[col], bins=edges, labels=labels, right=False)
        dummies = pd.get_dummies(cat, prefix=f"bin_{col}")
        out = pd.concat([out, dummies], axis=1)
    return out

def apply_rules(df: pd.DataFrame, rules: dict[str, tuple[float, float]]) -> pd.DataFrame:
    out = df.copy()
    for col, (lo, hi) in rules.items():
        if col in out:
            out[col] = out[col].clip(lower=lo, upper=hi)
    return out
