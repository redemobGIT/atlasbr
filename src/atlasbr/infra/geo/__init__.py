"""
AtlasBR - Geo Infrastructure Exports.

Uses lazy loading to prevent eager import of heavy dependencies (like geobr).
"""
import importlib
from typing import List

__all__ = [
    "fetch_tracts_raw",
    "fetch_urban_area_raw_gdf",
    "resolve_places_to_ids",
]

_LAZY_IMPORTS = {
    "fetch_tracts_raw": ".tracts",
    "fetch_urban_area_raw_gdf": ".footprint",
    "resolve_places_to_ids": ".resolver",
}


def __getattr__(name: str):
    """Lazily import modules when their attributes are requested."""
    if name in _LAZY_IMPORTS:
        module_name = _LAZY_IMPORTS[name]
        module = importlib.import_module(module_name, package=__name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> List[str]:
    """Expose lazy attributes to dir() for autocompletion."""
    return sorted(list(globals().keys()) + list(_LAZY_IMPORTS.keys()))