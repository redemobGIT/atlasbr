"""
AtlasBR - Infrastructure Geo Adapter (Place Resolver).

Resolves place inputs (IDs or Names) to standard 7-digit IBGE codes.
Uses 'geobr' for authoritative name resolution and fuzzy matching.
"""
import pandas as pd
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

@lru_cache(maxsize=1)
def _fetch_muni_metadata() -> pd.DataFrame:
    """
    Fetches the full municipality list from geobr (Cached).
    Includes fixes for encoding artifacts common on Windows.
    """
    try:
        import geobr
        from unidecode import unidecode
    except ImportError as e:
        raise ImportError(
            "Place resolution by name requires 'geobr' and 'unidecode'. "
            "Please install them via `pip install atlasbr[geo]`."
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
        df["norm_name"] = df["name_muni"].apply(lambda x: unidecode(str(x)).lower().strip())
        df["norm_uf"] = df["abbrev_state"].apply(lambda x: unidecode(str(x)).lower().strip())
        
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
    lookup = None # Lazy load
    resolved_ids: List[int] = []
    seen: Set[int] = set()

    # Import locally to avoid eager dependency
    from unidecode import unidecode

    for item in places:
        # --- STRATEGY 1: Is it a Code? ---
        # Handles Ints, Floats, and Numeric Strings
        try:
            # Check if it's a number (or string that looks like one)
            is_numeric = isinstance(item, (int, float)) or (isinstance(item, str) and item.strip().replace('.', '').isdigit())
            
            if is_numeric:
                code = int(float(item))
                if code not in seen:
                    seen.add(code)
                    resolved_ids.append(code)
                continue # Done with this item
        except (ValueError, TypeError):
            # Not a code, fall through to name resolution
            pass

        # --- STRATEGY 2: Is it a Name? ---
        # Requires geobr lookup
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
            
            s_name = unidecode(clean_name).lower()
            s_uf = unidecode(clean_uf).lower()
            
            # Exact Match
            match = lookup[
                (lookup["norm_name"] == s_name) & 
                (lookup["norm_uf"] == s_uf)
            ]
            
            if not match.empty:
                code = int(match.iloc[0]["code_muni"])
                if code not in seen:
                    logger.info(f"    ℹ️  Resolved '{clean_name}, {clean_uf}' -> {code}")
                    seen.add(code)
                    resolved_ids.append(code)
            else:
                # Fuzzy "Did you mean?" Logic
                state_matches = lookup[lookup["norm_uf"] == s_uf]
                if not state_matches.empty:
                    possibilities = state_matches[
                        state_matches["norm_name"].str.contains(s_name[:4], na=False)
                    ]["name_muni"].head(5).tolist()
                    
                    msg = f"Could not resolve municipality: '{clean_name}' in '{clean_uf}'."
                    if possibilities:
                        msg += f" Did you mean: {possibilities}?"
                    else:
                        msg += f" (State {clean_uf.upper()} found, but no matching city)."
                    raise ValueError(msg)
                else:
                    raise ValueError(f"Could not find state '{clean_uf}'. Check abbreviation (e.g., 'RJ').")
        else:
            raise ValueError(f"Invalid place format: '{item}'. Use ID (3304557) or 'Name, UF'.")

    return resolved_ids