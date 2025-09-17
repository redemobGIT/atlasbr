from __future__ import annotations
import pandas as pd

CANON_DTYPES: dict[str, str] = {
    "id_setor": "string",
    "cod_municipio": "string",
    "ano": "int16",
    "pop_total": "int32",
    "renda_media": "float32",
}

def standardize_ids(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "id_setor" in out:
        out["id_setor"] = out["id_setor"].astype("string").str.zfill(15)
    return out

def coerce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col, dt in CANON_DTYPES.items():
        if col in out:
            out[col] = out[col].astype(dt)
    return out

def basic_qc(df: pd.DataFrame, required: list[str]) -> dict[str, object]:
    missing = [c for c in required if c not in df.columns]
    nulls = {c: int(df[c].isna().sum()) for c in df.columns}
    return {"missing": missing, "nulls": nulls, "rows": len(df)}
