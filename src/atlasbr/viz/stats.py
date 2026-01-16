from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd

from atlasbr.viz.validators import coerce_numeric, require_columns


def describe_numeric(
    df: pd.DataFrame,
    *,
    cols: Iterable[str],
    quantiles: Optional[Iterable[float]] = None,
) -> pd.DataFrame:
    q = list(quantiles or [0.05, 0.5, 0.95])
    cols = list(cols)
    require_columns(df, cols)

    out = {}
    for c in cols:
        s = coerce_numeric(df[c], fillna=None).dropna()
        out[c] = {
            "count": int(s.shape[0]),
            "mean": float(s.mean()) if len(s) else float("nan"),
            "std": float(s.std()) if len(s) else float("nan"),
            "min": float(s.min()) if len(s) else float("nan"),
            "max": float(s.max()) if len(s) else float("nan"),
            **{f"q{int(p*100):02d}": float(s.quantile(p)) for p in q},
        }
    return pd.DataFrame(out).T


def describe_grouped(
    df: pd.DataFrame,
    *,
    value: str,
    group: str,
    agg: str = "sum",
) -> pd.DataFrame:
    require_columns(df, [value, group])
    v = coerce_numeric(df[value], fillna=0.0)
    g = df[group].astype("object")

    if agg == "sum":
        s = v.groupby(g, dropna=False).sum()
    elif agg == "mean":
        s = v.groupby(g, dropna=False).mean()
    elif agg == "count":
        s = g.groupby(g, dropna=False).size()
    else:
        raise ValueError(f"Unknown agg: {agg!r}")

    return s.sort_values(ascending=False).rename(agg).to_frame()
