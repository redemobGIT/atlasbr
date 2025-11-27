"""
AtlasBR - Infrastructure Geo Adapter (Place Resolver).
"""
import pandas as pd
import geobr
from typing import List, Set
from functools import lru_cache
from unidecode import unidecode
from atlasbr.settings import logger
from atlasbr.core.types import PlaceInput

def _fix_encoding(text: str) -> str:
    """
    Repairs common mojibake (UTF-8 bytes interpreted as Windows-1252).
    Example: 'NiterÃ³i' -> 'Niterói'
    """
    if not isinstance(text, str):
        return text
    try:
        # Check if it looks like UTF-8 bytes decoded as cp1252/latin1
        # This double-flip fixes the artifact
        return text.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        # If it fails (meaning it wasn't broken, or broken differently), return as is
        return text

@lru_cache(maxsize=1)
def _fetch_muni_metadata() -> pd.DataFrame:
    """
    Fetches the full municipality list from geobr (Cached).
    Includes fixes for encoding artifacts common on Windows.
    """
    logger.info("Fetching municipality metadata from geobr...")
    try:
        df = geobr.lookup_muni(code_muni="all", verbose=False)
        
        if df.empty:
            raise RuntimeError("geobr returned an empty lookup table.")

        # 1. Fix Encoding Artifacts (The "NiterÃ³i" bug)
        # We apply this to names to ensure they are valid UTF-8
        df["name_muni"] = df["name_muni"].apply(_fix_encoding)
        df["abbrev_state"] = df["abbrev_state"].apply(_fix_encoding)

        # 2. Normalize for Matching
        # "Niterói" -> "niteroi", "RJ" -> "rj"
        df["norm_name"] = df["name_muni"].apply(lambda x: unidecode(str(x)).lower().strip())
        df["norm_uf"] = df["abbrev_state"].apply(lambda x: unidecode(str(x)).lower().strip())
        
        return df
        
    except Exception as e:
        raise RuntimeError(f"Failed to fetch municipality list: {e}")

def resolve_places_to_ids(places: List[PlaceInput]) -> List[int]:
    """Resolves mixed input types to unique 7-digit IBGE IDs."""
    lookup = _fetch_muni_metadata()
    resolved_ids: Set[int] = set()

    for item in places:
        # 1. Handle ID inputs
        if isinstance(item, int) or (isinstance(item, str) and item.strip().isdigit()):
            resolved_ids.add(int(item))
            continue
        
        # 2. Handle Name inputs
        name, uf = None, None
        
        if isinstance(item, tuple) and len(item) == 2:
            name, uf = item
        elif isinstance(item, str) and "," in item:
            parts = item.split(",")
            if len(parts) >= 2:
                name, uf = parts[0], parts[1]
        
        if name and uf:
            # Strip whitespace immediately to avoid " RJ" errors in messages
            clean_name = name.strip()
            clean_uf = uf.strip()
            
            s_name = unidecode(clean_name).lower()
            s_uf = unidecode(clean_uf).lower()
            
            match = lookup[
                (lookup["norm_name"] == s_name) & 
                (lookup["norm_uf"] == s_uf)
            ]
            
            if not match.empty:
                resolved_ids.add(int(match.iloc[0]["code_muni"]))
            else:
                # 3. Smart Error Message
                state_matches = lookup[lookup["norm_uf"] == s_uf]
                if not state_matches.empty:
                    # Search for substring match to give better hints
                    possibilities = state_matches[
                        state_matches["norm_name"].str.contains(s_name[:4], na=False)
                    ]["name_muni"].head(5).tolist()
                    
                    msg = f"Could not resolve municipality: '{clean_name}' in '{clean_uf}'."
                    if possibilities:
                        msg += f" Did you mean one of these in {clean_uf.upper()}? {possibilities}"
                    else:
                        msg += f" (State {clean_uf.upper()} found, but no matching city)."
                    raise ValueError(msg)
                else:
                    raise ValueError(f"Could not find state '{clean_uf}'. Check your abbreviation (e.g., 'RJ', 'SP').")
        else:
            raise ValueError(f"Invalid place format: '{item}'. Use ID (3304557) or 'Name, UF'.")

    return list(resolved_ids)