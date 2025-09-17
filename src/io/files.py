from __future__ import annotations
from pathlib import Path
import pandas as pd, geopandas as gpd

def load_file(path: str):
    """Lê arquivo local (CSV/Parquet/Geo) e retorna DataFrame/GeoDataFrame."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    suf = p.suffix.lower()
    if suf in {".csv", ".txt"}:
        return pd.read_csv(p)
    if suf == ".parquet":
        return pd.read_parquet(p)
    return gpd.read_file(p)
