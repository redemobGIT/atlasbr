import re
import pandas as pd
from typing import Sequence


def filter_states(df, siglas):
    siglas = [siglas] if isinstance(siglas, str) else siglas
    return df[df["SIGLA"].isin(siglas)].copy()


def filter_years(df, start=None, end=None):
    if start:
        df = df[df["ANO"] >= start]
    if end:
        df = df[df["ANO"] <= end]
    return df


def _has_digits(s):
    return bool(re.search(r"\d|POP", s))


def _ends_with_any(suffixes):
    return lambda s: any(s.endswith(sfx) for sfx in suffixes)


def _starts_with(prefix):
    return lambda s: s.startswith(prefix)


def select_columns(df, by_age=False, gender=False):
    cols = df.columns
    base = _has_digits if by_age else _starts_with("POP")
    suffixes = ["_M", "_H"] if gender else ["_T"]
    suffix = _ends_with_any(suffixes)
    return [col for col in cols if base(col) and suffix(col)]


def rename_columns(cols):
    gender_map = {"_H": "Homens", "_M": "Mulheres", "_T": "Total"}

    def rename(col):
        for sfx, g in gender_map.items():
            if col.endswith(sfx):
                base = col.removesuffix(sfx)
                return f"{base} • {g}" if re.search(r"\d", base) else g
        return col

    return {col: rename(col) for col in cols}


def reshape_for_plot(df, columns):
    labels = rename_columns(columns)
    melted = df.melt(
        id_vars=["ANO", "SIGLA"],
        value_vars=columns,
        var_name="Grupo",
        value_name="População",
    )
    melted["Grupo"] = melted["Grupo"].map(labels)
    melted["População"] = melted["População"] / 1_000  # scale to thousands
    return melted
