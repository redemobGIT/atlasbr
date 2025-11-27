"""
AtlasBR - Infrastructure Geo Adapter (Place Resolver).
"""
import pandas as pd
import geobr
from typing import List, Union, Tuple, Set
from functools import lru_cache
from unidecode import unidecode
from atlasbr.settings import logger

PlaceInput = Union[int, str, Tuple[str, str]]

@lru_cache(maxsize=1)
def _fetch_muni_metadata() -> pd.DataFrame:
    """Fetches the full municipality list from geobr (Cached)."""
    logger.info("Fetching municipality metadata from geobr...")
    try:
        return geobr.lookup_muni(code_muni="all", verbose=False)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch municipality list: {e}")

def resolve_places_to_ids(places: List[PlaceInput]) -> List[int]:
    """Resolves mixed input types to unique 7-digit IBGE IDs."""
    lookup = _fetch_muni_metadata()
    
    # Normalize lookup table for matching
    lookup["norm_name"] = lookup["name_muni"].apply(lambda x: unidecode(str(x)).lower().strip())
    lookup["norm_uf"] = lookup["abbrev_state"].apply(lambda x: unidecode(str(x)).lower().strip())
    
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
            s_name = unidecode(name).lower().strip()
            s_uf = unidecode(uf).lower().strip()
            
            match = lookup[
                (lookup["norm_name"] == s_name) & 
                (lookup["norm_uf"] == s_uf)
            ]
            
            if not match.empty:
                resolved_ids.add(int(match.iloc[0]["code_muni"]))
            else:
                raise ValueError(f"Could not resolve municipality: '{name}' in '{uf}'")
        else:
            raise ValueError(f"Invalid place format: '{item}'")

    return list(resolved_ids)