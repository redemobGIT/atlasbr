from __future__ import annotations
from pathlib import Path
import io, zipfile, requests
import pandas as pd
import geopandas as gpd

def load_http(url: str, *, fmt: str | None = None):
    """Baixa dados via HTTP/HTTPS e retorna DataFrame/GeoDataFrame."""
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    content = r.content
    fmt = (fmt or url.split("?")[0].split("#")[0].split(".")[-1]).lower()

    if fmt == "zip":
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            members = zf.namelist()
            csvs = [m for m in members if m.lower().endswith(".csv")]
            pars = [m for m in members if m.lower().endswith(".parquet")]
            target = csvs[0] if csvs else (pars[0] if pars else members[0])
            with zf.open(target) as fh:
                if target.lower().endswith(".csv"):
                    return pd.read_csv(fh)
                if target.lower().endswith(".parquet"):
                    return pd.read_parquet(fh)
        raise ValueError("ZIP sem CSV/Parquet.")

    if fmt in {"csv","txt"}:
        return pd.read_csv(io.BytesIO(content))
    if fmt == "parquet":
        return pd.read_parquet(io.BytesIO(content))
    if fmt in {"geojson","json","shp","gpkg"}:
        tmp = Path("/tmp/_urbikit_http."+fmt)
        tmp.write_bytes(content)
        try:
            return gpd.read_file(tmp)
        finally:
            tmp.unlink(missing_ok=True)
    raise ValueError(f"Formato não suportado: {fmt}")
