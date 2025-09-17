from __future__ import annotations
import pandas as pd
from urbikit import clean

_RENAME = {
    "CO_ENTIDADE": "id_escola",
    "CO_MUNICIPIO": "cod_municipio",
    "SG_UF": "sigla_uf",
    "NO_ENTIDADE": "nome_escola",
}

_DTYPES = {
    "id_escola": "string",
    "cod_municipio": "string",
    "sigla_uf": "string",
    "ano": "int16",
}

def normalize_escolas(df_raw: pd.DataFrame, *, ano: int) -> pd.DataFrame:
    """Normalize INEP school census to canonical schema."""
    df = df_raw.rename(columns=_RENAME)
    if "ano" not in df:
        df["ano"] = ano
    df = clean.standardize_ids(df)
    df = clean.coerce_dtypes(df)
    for c, dt in _DTYPES.items():
        if c in df:
            df[c] = df[c].astype(dt)
    qc = clean.basic_qc(df, required=["id_escola", "cod_municipio", "sigla_uf", "ano"])
    if qc["missing"]:
        raise ValueError(f"Missing columns: {qc['missing']}")
    return df
