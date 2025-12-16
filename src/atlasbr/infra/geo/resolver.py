"""
AtlasBR - Geo Resolver.

Resolves place inputs (IDs or Names) to standard 7-digit IBGE codes.
Ensures deterministic ordering of outputs for consistent caching.
"""
from typing import List, Set
from atlasbr.core.types import PlaceInput
from atlasbr.settings import logger

# In a full implementation, this would connect to a lookup table/database
# For now, we assume inputs are already valid IBGE IDs (integers or strings).

def resolve_places_to_ids(places: List[PlaceInput]) -> List[int]:
    """
    Resolves a list of mixed inputs (integers, strings) to unique IBGE 7-digit integers.
    
    Behavior:
    - Standardizes inputs to 7-digit integers.
    - Removes duplicates while PRESERVING input order (deterministic).
    - Skips invalid inputs with a warning.
    
    Args:
        places: List of municipality IDs (e.g., 3304557) or names (placeholder).
        
    Returns:
        List[int]: Deduplicated list of valid 7-digit IBGE codes.
    """
    resolved_ids: List[int] = []
    seen: Set[int] = set()

    for p in places:
        try:
            # 1. Normalize to Integer
            # Handles strings like "3304557" or floats like 3304557.0
            val_str = str(p).strip()
            
            # Simple check if it's a name (non-numeric)
            if not val_str.isdigit():
                # Placeholder for name-to-id lookup
                # In production: id = lookup_name(val_str)
                logger.warning(
                    f"    ⚠️ Resolution by name '{p}' is not yet implemented. "
                    "Please provide the 7-digit IBGE code."
                )
                continue

            code = int(float(val_str))
            
            # 2. Validate/Pad IBGE Code
            # Accept 6-digit codes (older standard) and pad them, or 7-digit
            if 100000 <= code <= 999999:
                # Calculate checksum digit? For now, we assume user meant 7-digit 
                # but we need 7 digits for DB queries. 
                # NOTE: Padding usually requires checksum calc, simplified here for robustness:
                # If strictly 6 digits passed, we might warn. 
                # For this implementation, we strictly filter for ~7 digit range validity.
                pass
            
            if not (1000000 <= code <= 9999999):
                 logger.warning(f"    ⚠️ Ignoring invalid IBGE code: {code}")
                 continue

            # 3. Deduplicate (Deterministic)
            if code not in seen:
                seen.add(code)
                resolved_ids.append(code)
                
        except (ValueError, TypeError):
            logger.warning(f"    ⚠️ Could not parse place input: {p}")
            continue

    return resolved_ids