"""
AtlasBR - Infrastructure Geo Adapter (Place Resolver).

Resolves place inputs (IDs or Names) to standard 7-digit IBGE codes.
Uses 'geobr' for authoritative name resolution and fuzzy matching.
"""
import pandas as pd
import unicodedata
from typing import List, Set, Any
from functools import lru_cache

from atlasbr.settings import logger
from atlasbr.core.types import PlaceInput


def _fix_encoding(text: Any) -> str:
    """
    Repairs common mojibake (UTF-8 bytes interpreted as Windows-1252).
    Example: 'NiterÃ³i' -> 'Niterói'
    """
    if not isinstance(text, str):
        return str(text)
    try:
        # Check if it looks like UTF-8 bytes decoded as cp1252/latin1
        return text.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def _normalize_text(text: Any) -> str:
    """
    Normalizes text for comparison (remove accents, lowercase).
    Tries 'unidecode' first, falls back to 'unicodedata'.
    """
    # 1. Fix encoding artifacts first
    s = _fix_encoding(text)

    # 2. Try unidecode (better for transliteration)
    try:
        from unidecode import unidecode
        return unidecode(s).lower().strip()
    except ImportError:
        pass

    # 3. Fallback: NFD decomposition (strip accents)
    s = unicodedata.normalize('NFD', s)
    s = s.encode('ascii', 'ignore').decode('utf-8')
    return s.lower().strip()


@lru_cache(maxsize=1)
def _fetch_muni_metadata() -> pd.DataFrame:
    """
    Fetches the full municipality list from geobr (Cached).
    Includes fixes for encoding artifacts common on Windows.
    """
    try:
        import geobr
    except ImportError as e:
        raise ImportError(
            "Place resolution by name requires 'geobr'. "
            "Please install it via `pip install atlasbr[geo]`."
        ) from e

    logger.info("    🌍 Fetching municipality metadata from geobr...")
    try:
        # verbose=False silences the geobr download progress bar
        df = geobr.lookup_muni(code_muni="all", verbose=False)

        if df.empty:
            raise RuntimeError("geobr returned an empty lookup table.")

        # 1. Fix Encoding Artifacts
        df["name_muni"] = df["name_muni"].apply(_fix_encoding)
        df["abbrev_state"] = df["abbrev_state"].apply(_fix_encoding)

        # 2. Normalize for Matching ("Niterói" -> "niteroi")
        # Uses robust helper that works without unidecode
        df["norm_name"] = df["name_muni"].apply(_normalize_text)
        df["norm_uf"] = df["abbrev_state"].apply(_normalize_text)

        return df

    except Exception as e:
        raise RuntimeError(f"Failed to fetch municipality list: {e}")


def resolve_places_to_ids(places: List[PlaceInput]) -> List[int]:
    """
    Resolves mixed input types to unique 7-digit IBGE IDs.

    Supported Inputs:
    - Codes: 3304557 (int), 3304557.0 (float), "3304557" (str)
    - Names: "Niterói, RJ", ("Niterói", "RJ")
    """
    lookup = None  # Lazy load
    resolved_ids: List[int] = []
    seen: Set[int] = set()

    for item in places:
        # --- STRATEGY 1: Is it a Code? ---
        # Handles Ints, Floats, and Numeric Strings WITHOUT importing geobr
        try:
            is_numeric = (
                isinstance(item, (int, float)) or
                (isinstance(item, str) and
                 item.strip().replace('.', '').isdigit())
            )

            if is_numeric:
                code = int(float(item))
                if code not in seen:
                    seen.add(code)
                    resolved_ids.append(code)
                continue  # Done with this item
        except (ValueError, TypeError):
            pass

        # --- STRATEGY 2: Is it a Name? ---
        # Requires geobr lookup (lazy load)
        if lookup is None:
            lookup = _fetch_muni_metadata()

        name, uf = None, None

        # Parse "City, UF" or ("City", "UF")
        if isinstance(item, tuple) and len(item) == 2:
            name, uf = item
        elif isinstance(item, str) and "," in item:
            parts = item.split(",")
            if len(parts) >= 2:
                name, uf = parts[0], parts[1]

        if name and uf:
            clean_name = name.strip()
            clean_uf = uf.strip()

            s_name = _normalize_text(clean_name)
            s_uf = _normalize_text(clean_uf)

            # Exact Match
            match = lookup[
                (lookup["norm_name"] == s_name) &
                (lookup["norm_uf"] == s_uf)
            ]

            if not match.empty:
                code = int(match.iloc[0]["code_muni"])
                if code not in seen:
                    logger.info(
                        f"    ℹ️  Resolved '{clean_name}, {clean_uf}' "
                        f"-> {code}"
                    )
                    seen.add(code)
                    resolved_ids.append(code)
            else:
                # Fuzzy "Did you mean?" Logic
                state_matches = lookup[lookup["norm_uf"] == s_uf]
                if not state_matches.empty:
                    # Simple containment check since we might lack fuzzywuzzy
                    mask = state_matches["norm_name"].str.contains(
                        s_name[:4], na=False
                    )
                    possibilities = (
                        state_matches[mask]["name_muni"]
                        .head(5).tolist()
                    )

                    msg = (
                        f"Could not resolve municipality: '{clean_name}' "
                        f"in '{clean_uf}'."
                    )
                    if possibilities:
                        msg += f" Did you mean: {possibilities}?"
                    else:
                        msg += (
                            f" (State {clean_uf.upper()} found, "
                            "but no matching city)."
                        )
                    raise ValueError(msg)
                else:
                    raise ValueError(
                        f"Could not find state '{clean_uf}'. "
                        "Check abbreviation (e.g., 'RJ')."
                    )
        else:
            raise ValueError(
                f"Invalid place format: '{item}'. "
                "Use ID (3304557) or 'Name, UF'."
            )

    return resolved_ids