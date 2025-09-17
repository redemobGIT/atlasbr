from __future__ import annotations
from ftplib import FTP
from io import BytesIO
from pathlib import Path
import pandas as pd, geopandas as gpd, tempfile

def load_ftp(host: str, path: str, *, fmt: str | None = None, user: str = "anonymous", passwd: str = ""):
    """Baixa arquivo via FTP e retorna DataFrame/GeoDataFrame."""
    with FTP(host) as ftp:
        ftp.login(user=user, passwd=passwd)
        ftp.set_pasv(True)
        bio = BytesIO()
        ftp.retrbinary(f"RETR {path}", bio.write)
    content = bio.getvalue()
    fmt = (fmt or Path(path).suffix.lstrip(".")).lower()
    if fmt in {"csv","txt"}:
        return pd.read_csv(BytesIO(content))
    if fmt == "parquet":
        return pd.read_parquet(BytesIO(content))
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)/f"ftp.{fmt}"
        tmp.write_bytes(content)
        return gpd.read_file(tmp)
