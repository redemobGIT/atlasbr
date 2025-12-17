"""
AtlasBR - Infrastructure Adapter for IBGE FTP.

Handles downloading, unzipping, and parsing raw CSV files from IBGE's FTP.
Includes logic for dynamic revision date resolution (2010) and robust
header parsing.
"""

import zipfile
import fnmatch
import re
import pandas as pd
import numpy as np
from io import BytesIO
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import List, Set, Tuple, Optional

from atlasbr.core.catalog.census import CensusThemeSpec
from atlasbr.settings import logger
from atlasbr.infra.storage.cache import cached_download, url_to_filename

# IBGE Code to State Abbreviation Mapping
STATE_MAP = {
    11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
    21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL",
    28: "SE", 29: "BA", 31: "MG", 32: "ES", 33: "RJ", 35: "SP", 41: "PR",
    42: "SC", 43: "RS", 50: "MS", 51: "MT", 52: "GO", 53: "DF"
}

# 2010 special cases
SP_STATE_CODE = 35
SP_CAPITAL_MUNI = 3550308  # São Paulo (capital) municipality code (7 digits)


@lru_cache(maxsize=32)
def _ibge_dir_zip_listing(dir_url: str) -> List[str]:
    """Return ZIP filenames present in an IBGE directory listing."""
    rel = Path("ibge") / "census" / url_to_filename(dir_url, suffix=".html")
    html_path = cached_download(dir_url, relpath=rel, timeout=60)
    html = html_path.read_bytes().decode("latin-1", errors="ignore")
    return re.findall(r'href="([^"]+\.zip)"', html, flags=re.IGNORECASE)


def _pick_zip_for_stem(dir_url: str, stem: str) -> str:
    """
    Resolve the concrete ZIP URL for a logical stem (e.g., 'RJ').

    IBGE publishes files as <STEM>_YYYYMMDD.zip. The date varies by state.
    """
    files = _ibge_dir_zip_listing(dir_url)
    stem_re = re.escape(stem)

    candidates: List[Tuple[str, str]] = []
    for f in files:
        name = PurePosixPath(f).name
        # Match standard pattern: STEM_20110101.zip
        m = re.fullmatch(
            rf"{stem_re}_(\d{{8}})\.zip", name, flags=re.IGNORECASE
        )
        if m:
            candidates.append((m.group(1), name))
            continue
        # Match exact stem: STEM.zip
        if re.fullmatch(rf"{stem_re}\.zip", name, flags=re.IGNORECASE):
            candidates.append(("00000000", name))

    # Fuzzy fallbacks for known IBGE naming variants (mainly SP)
    if not candidates and stem.upper() == "SP_CAPITAL":
        for f in files:
            name = PurePosixPath(f).name
            m = re.fullmatch(
                r"SP_.*Capital_(\d{8})\.zip", name, flags=re.IGNORECASE
            )
            if m and "EXCETO" not in name.upper():
                candidates.append((m.group(1), name))

    if not candidates and stem.upper().startswith("SP_EXCETO"):
        for f in files:
            name = PurePosixPath(f).name
            m = re.fullmatch(
                r"SP_Exceto.*Capital_(\d{8})\.zip", name, flags=re.IGNORECASE
            )
            if m:
                candidates.append((m.group(1), name))

    if not candidates:
        raise FileNotFoundError(f"No ZIP found for stem='{stem}' at {dir_url}")

    # Pick the latest date
    _, fname = max(candidates, key=lambda x: x[0])
    return dir_url.rstrip("/") + "/" + fname


def _stems_for_state_2010(uf: str, munis: List[int]) -> List[str]:
    """Return the list of IBGE 2010 ZIP stems needed for a UF."""
    uf = uf.upper()
    if uf != "SP":
        return [uf]

    has_capital = SP_CAPITAL_MUNI in munis
    has_interior = any(
        (
            str(m).zfill(7).startswith(str(SP_STATE_CODE))
            and m != SP_CAPITAL_MUNI
        )
        for m in munis
    )

    stems: List[str] = []
    if has_capital:
        stems.append("SP_Capital")
    if has_interior or not has_capital:
        stems.append("SP_Exceto_a_Capital")
    return stems


@lru_cache(maxsize=32)
def _download_zip_ftp(url: str) -> BytesIO:
    """Downloads a ZIP file from a URL to the local cache."""
    rel = Path("ibge") / "census" / url_to_filename(url, suffix=".zip")
    zip_path = cached_download(url, relpath=rel, timeout=180)
    return BytesIO(zip_path.read_bytes())


def _resolve_target_urls(
    spec: CensusThemeSpec,
    munis: List[int],
) -> List[Tuple[str, str, str, object]]:
    """
    Generate a fetch plan for the requested municipalities.

    Returns:
        List of (url, member_glob, context, resource_spec).
    """
    required_states: Set[str] = set()
    for m_id in munis:
        state_code = int(str(m_id)[:2])
        uf = STATE_MAP.get(state_code)
        if uf:
            required_states.add(uf)
        else:
            logger.warning(f"    ⚠️ Unknown state code {m_id}. Skipping.")

    if not required_states:
        raise ValueError("No valid states found for the requested munis.")

    targets: List[Tuple[str, str, str, object]] = []

    for resource in spec.ftp_resources:
        template = resource.url_template
        glob_pattern = resource.member_glob or "*.csv"

        if ("{uf}" in template) or ("{stem}" in template):
            for uf in required_states:
                if spec.year == 2010:
                    # 2010 requires dynamic scraping of the directory
                    dir_url = template.rsplit("/", 1)[0] + "/"
                    for stem in _stems_for_state_2010(uf, munis):
                        try:
                            url = _pick_zip_for_stem(dir_url, stem)
                            targets.append((url, glob_pattern, stem, resource))
                        except Exception as e:
                            logger.error(f"Failed resolving {stem}: {e}")
                else:
                    # Standard substitution
                    url = template.format(uf=uf, UF=uf, stem=uf)
                    targets.append((url, glob_pattern, uf, resource))
        else:
            targets.append((template, glob_pattern, "BR", resource))

    return targets


def _match_zip_members(members: List[str], pattern: str) -> List[str]:
    """Match ZIP members using either full path or basename."""
    if "/" in pattern:
        out = [m for m in members if fnmatch.fnmatch(m, pattern)]
        if out:
            return out
        return [
            m for m in members if fnmatch.fnmatch(m.lower(), pattern.lower())
        ]

    out = [
        m for m in members if fnmatch.fnmatch(PurePosixPath(m).name, pattern)
    ]
    if out:
        return out
    return [
        m
        for m in members
        if fnmatch.fnmatch(PurePosixPath(m).name.lower(), pattern.lower())
    ]


def _resolve_usecols(
    zf: zipfile.ZipFile,
    member: str,
    desired: Optional[List[str]],
    *,
    sep: str,
    encoding: str,
) -> Optional[List[str]]:
    """Resolve desired columns against the actual header (case-insensitive)."""
    if not desired:
        return None

    # Read just the header
    header = pd.read_csv(
        zf.open(member),
        sep=sep,
        encoding=encoding,
        nrows=0,
        dtype=str,
    ).columns.tolist()

    header_map = {str(c).strip().lower(): c for c in header}

    resolved: List[str] = []
    missing: List[str] = []
    seen: Set[str] = set()

    for c in desired:
        key = str(c).strip().lower()
        actual = header_map.get(key)
        if not actual:
            missing.append(c)
            continue
        if actual not in seen:
            resolved.append(actual)
            seen.add(actual)

    if missing:
        logger.warning(
            f"    ⚠️ Missing cols in {PurePosixPath(member).name}: {missing}"
        )

    return resolved or None


def fetch_census_ftp(spec: CensusThemeSpec, munis: List[int]) -> pd.DataFrame:
    """Fetch Census data for the requested municipalities from IBGE FTP."""

    # 1) Resolve Fetch Plan
    fetch_plan = _resolve_target_urls(spec, munis)

    dfs: List[pd.DataFrame] = []

    # 2) Pre-calculate column requirements
    load_cols: Optional[List[str]] = None
    if spec.column_map:
        load_cols = list(spec.column_map.keys())
        for res in spec.ftp_resources:
            if res.id_col not in load_cols:
                load_cols.append(res.id_col)

    # 3) Execute downloads
    for url, glob_pattern, uf_context, resource in fetch_plan:
        logger.info(
            f"    ⬇️  Fetching {spec.theme} ({uf_context}) from IBGE FTP..."
        )

        try:
            zip_bytes = _download_zip_ftp(url)

            with zipfile.ZipFile(zip_bytes) as zf:
                candidates = _match_zip_members(zf.namelist(), glob_pattern)

                if not candidates:
                    logger.warning(
                        f"    ⚠️ No file matching '{glob_pattern}' "
                        f"in {uf_context} ZIP. Skipping."
                    )
                    continue

                for csv_filename in candidates:
                    res_spec = resource

                    try:
                        # Robust column resolution (handles V002 vs v002)
                        usecols = _resolve_usecols(
                            zf,
                            csv_filename,
                            load_cols,
                            sep=res_spec.sep,
                            encoding=res_spec.encoding,
                        )

                        df_chunk = pd.read_csv(
                            zf.open(csv_filename),
                            sep=res_spec.sep,
                            encoding=res_spec.encoding,
                            usecols=usecols,
                            dtype=str,
                            na_values=["X", ".", "nan", "..", "...", "-"],
                            keep_default_na=True,
                        )

                        if spec.column_map:
                            df_chunk = df_chunk.rename(columns=spec.column_map)

                        if "id_setor_censitario" in df_chunk.columns:
                            # Standardize ID
                            df_chunk["id_setor_censitario"] = (
                                df_chunk["id_setor_censitario"]
                                .astype(str)
                                .str.strip()
                                .str.zfill(15)
                            )

                            # Filter rows belonging to requested munis
                            df_chunk["_muni_temp"] = (
                                df_chunk["id_setor_censitario"].str[:7]
                            )
                            str_munis = {str(m).zfill(7) for m in munis}
                            df_chunk = df_chunk[
                                df_chunk["_muni_temp"].isin(str_munis)
                            ]

                            df_chunk = df_chunk.drop(
                                columns=["_muni_temp"]
                            ).set_index("id_setor_censitario")

                        # Numeric Conversion
                        for col in df_chunk.columns:
                            if df_chunk[col].dtype == "object":
                                df_chunk[col] = df_chunk[col].str.replace(
                                    ",", ".", regex=False
                                )
                                df_chunk[col] = pd.to_numeric(
                                    df_chunk[col], errors="coerce"
                                )

                        if not df_chunk.empty:
                            dfs.append(df_chunk)

                    except Exception as e:
                        logger.warning(
                            f"    ⚠️ Failed parsing {uf_context}/"
                            f"{PurePosixPath(csv_filename).name}: {e}"
                        )
                        continue

        except Exception as e:
            logger.error(f"    ❌ Failed to process {uf_context}: {e}")
            continue

    if not dfs:
        logger.error("    ❌ No data found for any requested municipality.")
        return pd.DataFrame()

    return pd.concat(dfs)   