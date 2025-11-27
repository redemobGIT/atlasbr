"""
AtlasBR - Geo Resolver.

Resolves user-friendly place names (e.g., "Rio de Janeiro, RJ") into 
canonical 7-digit IBGE IDs using `geobr` reference tables.
"""

import pandas as pd
import geobr
from typing import List, Union, Tuple, Set
from functools import lru_cache
from unidecode import unidecode

# Type alias for the accepted input formats
# Can be: 1234567, "1234567", ("Name", "UF"), or "Name, UF"
PlaceInput = Union[int, str, Tuple[str, str]]

@lru_cache(maxsize=1)
def _get_muni_lookup() -> pd.DataFrame:
    """
    Fetches the full master list of municipalities using geobr.
    We fetch 'all' once and cache it in memory for fast local querying.
    """
    print("    🔎 Fetching municipality metadata from geobr...")
    
    # Use lookup_muni with "all" to get the lightweight metadata table (no geometry)
    try:
        # Returns a DataFrame with columns: 
        # ['code_muni', 'name_muni', 'code_state', 'abbrev_state', 'name_state', ...]
        df = geobr.lookup_muni(code_muni="all", verbose=False)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch municipality list from geobr: {e}")

    # Normalize for case-insensitive matching
    # Create helper columns: 'norm_name' and 'norm_uf'
    df["norm_name"] = df["name_muni"].apply(lambda x: unidecode(str(x)).lower().strip())
    df["norm_uf"] = df["abbrev_state"].apply(lambda x: unidecode(str(x)).lower().strip())
    
    # Ensure code is int
    df["code_muni"] = df["code_muni"].astype(int)
    
    return df

def resolve_places(places: List[PlaceInput]) -> List[int]:
    """
    Resolves a mixed list of inputs into unique 7-digit IBGE IDs.
    
    Args:
        places: List containing ints, numeric strings, ("Name", "UF") tuples,
                or "Name, UF" strings.
    
    Returns:
        List[int]: List of unique 7-digit codes.
    
    Raises:
        ValueError: If a place cannot be found or the format is invalid.
    """
    lookup = _get_muni_lookup()
    resolved_ids: Set[int] = set()
    
    for item in places:
        # CASE 1: Already an ID (int or numeric string)
        if isinstance(item, int) or (isinstance(item, str) and item.strip().isdigit()):
            code = int(item)
            # Optional: Validate if ID exists in lookup
            if code not in lookup["code_muni"].values:
                print(f"        ⚠️ Warning: ID {code} not found in IBGE database. Keeping it anyway.")
            resolved_ids.add(code)
            continue
            
        # Parse Name/UF
        name, uf = None, None
        
        # CASE 2: Tuple ("Name", "UF")
        if isinstance(item, tuple) and len(item) == 2:
            name, uf = item
            
        # CASE 3: String "Name, UF"
        elif isinstance(item, str) and "," in item:
            parts = item.split(",")
            if len(parts) >= 2:
                name = parts[0]
                uf = parts[1]
        
        # Logic to find ID
        if name and uf:
            # Normalize input
            search_name = unidecode(name).lower().strip()
            search_uf = unidecode(uf).lower().strip()
            
            # Filter logic
            match = lookup[
                (lookup["norm_name"] == search_name) & 
                (lookup["norm_uf"] == search_uf)
            ]
            
            if match.empty:
                # Try fuzzy matching or helpful error
                possible_matches = lookup[lookup["norm_name"] == search_name]
                if not possible_matches.empty:
                    available_ufs = possible_matches["abbrev_state"].unique()
                    raise ValueError(f"City '{name}' not found in '{uf}'. Did you mean one of: {available_ufs}?")
                else:
                    raise ValueError(f"Could not resolve municipality: '{name}' in '{uf}'")
            
            # Add the found ID (taking the first match if multiple, though Name+UF should be unique)
            found_id = match.iloc[0]["code_muni"]
            resolved_ids.add(found_id)
            
        else:
            raise ValueError(f"Invalid place format: '{item}'. Use ID, ('Name', 'UF'), or 'Name, UF'.")
            
    return list(resolved_ids)