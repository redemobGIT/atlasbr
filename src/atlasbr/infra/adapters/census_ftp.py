"""
AtlasBR - Infrastructure Adapter for IBGE FTP.

This module handles downloading, unzipping, and parsing raw CSV files from IBGE's FTP servers.
It utilizes a local disk cache to prevent redundant downloads.
"""

import zipfile
import pandas as pd
import numpy as np
from io import BytesIO
from functools import lru_cache
from pathlib import Path

from atlasbr.core.catalog.census import CensusThemeSpec
from atlasbr.settings import logger
from atlasbr.infra.storage.cache import cached_download, url_to_filename


@lru_cache(maxsize=8)
def _download_zip_ftp(url: str) -> BytesIO:
    """
    Downloads a ZIP file from a URL to the local cache and returns it as a BytesIO object.
    
    The file is stored in <cache_dir>/ibge/census/<hash>.zip.
    """
    rel_path = Path("ibge") / "census" / url_to_filename(url, suffix=".zip")
    
    # Leverages the new disk cache utility
    zip_path = cached_download(url, relpath=rel_path, timeout=180)
    
    return BytesIO(zip_path.read_bytes())


@lru_cache(maxsize=1)
def fetch_income_ftp_2022(url: str) -> pd.DataFrame:
    """
    Downloads and parses the 2022 Income aggregates from IBGE.
    
    Args:
        url: Direct URL to the .zip file.

    Returns:
        pd.DataFrame: DataFrame with 'rendimento_medio', indexed by 'id_setor_censitario'.
    """
    logger.info(f"    ⬇️  Fetching Income 2022 from FTP (cached)...")

    # Use the shared cached downloader
    try:
        zip_bytes = _download_zip_ftp(url)
    except Exception as e:
         raise RuntimeError(f"Failed to retrieve Income data from {url}") from e

    with zipfile.ZipFile(zip_bytes) as zf:
        # Find the first CSV in the archive
        try:
            csv_filename = next(p for p in zf.namelist() if p.lower().endswith(".csv"))
        except StopIteration:
            raise FileNotFoundError("No CSV file found inside the IBGE ZIP archive.")

        # Read specific columns
        df = pd.read_csv(
            zf.open(csv_filename),
            sep=";",
            encoding="latin1",
            usecols=["CD_SETOR", "V06004"],
            dtype={"CD_SETOR": str},
        )

    # Clean and Standardize
    df = df.rename(
        columns={"CD_SETOR": "id_setor_censitario", "V06004": "rendimento_medio"}
    )

    # Fix formatting (IBGE uses 'X' for nulls and ',' for decimals)
    df["rendimento_medio"] = (
        df["rendimento_medio"]
        .astype(str)
        .str.replace(",", ".")
        .replace({"X": np.nan, ".": np.nan, "nan": np.nan})
    )

    df["rendimento_medio"] = pd.to_numeric(df["rendimento_medio"], errors="coerce")
    df["id_setor_censitario"] = df["id_setor_censitario"].str.zfill(15)

    return df.set_index("id_setor_censitario")


def fetch_census_ftp(spec: CensusThemeSpec) -> pd.DataFrame:
    """
    Generic fetcher for Census data based on a Theme Spec.
    """
    
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

    logger.info(f"    ⬇️  Fetching {spec.theme} {spec.year} from IBGE FTP (cached)...")
    
    try:
        zip_bytes = _download_zip_ftp(spec.url)
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve {spec.theme} data from {spec.url}") from e
        
    return _extract_and_clean(zip_bytes, spec)