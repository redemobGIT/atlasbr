"""
AtlasBR - Core Catalog Package.

Exposes the specification retrievers for all supported datasets.
"""

from .census import CENSUS_CATALOG, get_census_spec
from .rais import get_rais_spec
from .cnes import get_cnes_spec
from .inep import get_schools_spec

__all__ = [
    "CENSUS_CATALOG",
    "get_census_spec",
    "get_rais_spec",
    "get_cnes_spec",
    "get_schools_spec",
]