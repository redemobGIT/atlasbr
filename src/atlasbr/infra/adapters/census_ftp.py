"""
AtlasBR - Infrastructure Adapter for IBGE FTP.

This module handles downloading, unzipping, and parsing raw CSV files from IBGE's FTP servers.
Currently specialized for the 2022 Income dataset structure.
"""

import requests
import zipfile
import pandas as pd
import numpy as np
from io import BytesIO
from functools import lru_cache

from atlasbr.core.catalog.census import CensusThemeSpec


@lru_cache(maxsize=1)
def fetch_income_ftp_2022(url: str) -> pd.DataFrame:
    """
    Downloads and parses the 2022 Income aggregates from IBGE.

    This function:
    1. Downloads the ZIP file into memory.
    2. Finds the .csv inside.
    3. Parses it with latin1 encoding.
    4. Cleans numeric columns (converting ',' to '.').

    Args:
        url: Direct URL to the .zip file.

    Returns:
        pd.DataFrame: DataFrame with 'rendimento_medio', indexed by 'id_setor_censitario'.
    """
    print(f"    ⬇️  Downloading Income 2022 from FTP...")

    try:
        response = requests.get(url, timeout=120)  # Increased timeout for large files
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to download Census data from {url}") from e

    with zipfile.ZipFile(BytesIO(response.content)) as zf:
        # Find the first CSV in the archive
        try:
            csv_filename = next(p for p in zf.namelist() if p.lower().endswith(".csv"))
        except StopIteration:
            raise FileNotFoundError("No CSV file found inside the IBGE ZIP archive.")

        # Read the specific columns we care about to save memory
        # CD_SETOR = Sector ID
        # V06004 = Average Income (Rendimento médio mensal do responsável)
        df = pd.read_csv(
            zf.open(csv_filename),
            sep=";",
            encoding="latin1",
            usecols=["CD_SETOR", "V06004"],
            dtype={"CD_SETOR": str},  # Ensure ID is read as string
        )

    # Clean and Standardize
    # 1. Rename columns to match our internal catalog expectations somewhat
    #    (though mapping usually happens in logic, here we do structural cleanup)
    df = df.rename(
        columns={"CD_SETOR": "id_setor_censitario", "V06004": "rendimento_medio"}
    )

    # 2. Fix formatting (IBGE uses 'X' for nulls and ',' for decimals)
    df["rendimento_medio"] = (
        df["rendimento_medio"]
        .astype(str)
        .str.replace(",", ".")
        .replace({"X": np.nan, ".": np.nan, "nan": np.nan})
    )

    # 3. Convert to float
    df["rendimento_medio"] = pd.to_numeric(df["rendimento_medio"], errors="coerce")

    # 4. Ensure ID padding (IBGE sometimes drops leading zeros in CSVs)
    df["id_setor_censitario"] = df["id_setor_censitario"].str.zfill(15)

    return df.set_index("id_setor_censitario")


def fetch_census_ftp(spec: CensusThemeSpec) -> pd.DataFrame:
    def _download_zip_ftp(url: str) -> BytesIO:
        try:
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            return BytesIO(response.content)
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to download Census data from {url}") from e

    def _extract_and_clean(zip_bytes: BytesIO, spec: CensusThemeSpec) -> pd.DataFrame:
        with zipfile.ZipFile(zip_bytes) as zf:
            try:
                csv_filename = next(
                    p for p in zf.namelist() if p.lower().endswith(".csv")
                )
            except StopIteration:
                raise FileNotFoundError("No CSV file found inside ZIP.")

            cols_to_load = list(spec.column_map.keys()) if spec.column_map else None

            df = pd.read_csv(
                zf.open(csv_filename),
                sep=spec.csv_sep,
                encoding=spec.csv_encoding,
                usecols=cols_to_load,
                dtype=str,
                na_values=["X", ".", "nan", "..", "...", "-"],
                keep_default_na=True,
            )

            if spec.column_map:
                df = df.rename(columns=spec.column_map)

            if "id_setor_censitario" in df.columns:
                df["id_setor_censitario"] = df["id_setor_censitario"].str.zfill(15)
                df = df.set_index("id_setor_censitario")

            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].str.replace(",", ".", regex=False)
                    df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    print(f"    ⬇️  Downloading {spec.theme} {spec.year} from FTP...")
    zip_bytes = _download_zip_ftp(spec.url)
    return _extract_and_clean(zip_bytes, spec)
