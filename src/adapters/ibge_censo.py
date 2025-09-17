from __future__ import annotations
import pandas as pd
from urbikit import clean

_RENAME = {
    "id_setor_censitario": "id_setor",
    "cod_municipio": "cod_municipio",
    "sigla_uf": "sigla_uf",
    "pop_total": "pop_total",
}

_DTYPES = {
    "id_setor": "string",
    "cod_municipio": "string",
    "sigla_uf": "string",
    "ano": "int16",
    "pop_total": "int32",
}

def normalize_setores(df_raw: pd.DataFrame, *, ano: int) -> pd.DataFrame:
    """Normalize IBGE census sectors to canonical schema."""
    df = df_raw.rename(columns=_RENAME)
    if "ano" not in df:
        df["ano"] = ano
    df = clean.standardize_ids(df)
    for c, dt in _DTYPES.items():
        if c in df:
            df[c] = df[c].astype(dt)
    qc = clean.basic_qc(df, required=["id_setor", "cod_municipio", "sigla_uf", "ano"])
    if qc["missing"]:
        raise ValueError(f"Missing columns: {qc['missing']}")
    return df
