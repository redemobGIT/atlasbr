from __future__ import annotations

from typing import Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from atlasbr.viz.types import PlotStyle
from atlasbr.viz.validators import coerce_numeric, iter_groups, require_columns
from atlasbr.viz.validators import top_k_categories


def plot_distribution(
    df: pd.DataFrame,
    *,
    value: str,
    hue: Optional[str] = None,
    weights: Optional[str] = None,
    kind: str = "hist",
    transform: str = "log1p",
    normalize: str = "density",
    bins: int = 40,
    p_max: float = 0.99,
    top_k: Optional[int] = None,
    style: Optional[PlotStyle] = None,
    ax=None,
) -> Tuple:
    style = style or PlotStyle()
    cols = [value] + ([hue] if hue else []) + ([weights] if weights else [])
    require_columns(df, [c for c in cols if c])

    x = coerce_numeric(df[value], fillna=0.0)
    x = _transform(x, transform)

    hi = float(x.quantile(float(p_max))) if len(x) else 1.0
    edges = np.linspace(0.0, max(hi, 1e-9), int(bins) + 1)

    fig = None
    if ax is None:
        fig, ax = plt.subplots()

    if hue is None:
        _plot_hist(ax, x, edges, weights=_weights(df, weights), norm=normalize)
    else:
        mapped, cats = top_k_categories(df[hue], top_k=top_k)
        df2 = df.copy()
        df2[hue] = mapped
        for label, sub in iter_groups(df2, hue, cats):
            xs = coerce_numeric(sub[value], fillna=0.0)
            xs = _transform(xs, transform)
            _plot_hist(
                ax,
                xs,
                edges,
                weights=_weights(sub, weights),
                norm=normalize,
                label=label,
            )
        ax.legend()

    ax.set_title(style.title or "")
    ax.set_xlabel(style.xlabel or _xlabel(value, transform))
    ax.set_ylabel(style.ylabel or normalize.capitalize())

    if style.tight and fig is not None:
        fig.tight_layout()

    return fig, ax


def _weights(df: pd.DataFrame, weights: Optional[str]) -> Optional[pd.Series]:
    if weights is None:
        return None
    return coerce_numeric(df[weights], fillna=0.0)


def _plot_hist(ax, x, edges, *, weights=None, norm="density", label=None):
    density = norm == "density"
    ax.hist(
        x.to_numpy(),
        bins=edges,
        weights=None if weights is None else weights.to_numpy(),
        density=density,
        alpha=0.55,
        label=label,
    )


def _transform(x: pd.Series, transform: str) -> pd.Series:
    if transform == "none":
        return x
    if transform == "log1p":
        return np.log1p(x.clip(lower=0.0))
    if transform == "log10":
        return np.log10(1.0 + x.clip(lower=0.0))
    raise ValueError(f"Unknown transform: {transform!r}")


def _xlabel(value: str, transform: str) -> str:
    if transform == "none":
        return value
    if transform == "log1p":
        return f"log(1 + {value})"
    if transform == "log10":
        return f"log10(1 + {value})"
    return value
