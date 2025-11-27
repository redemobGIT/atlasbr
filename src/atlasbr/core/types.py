"""
AtlasBR - Shared Domain Types.
"""
from typing import Union, Tuple, List, Literal

# Represents a Municipality Identifier input
# Can be:
# - int/str ID: 3304557 or "3304557"
# - Tuple: ("Rio de Janeiro", "RJ")
# - String: "Niterói, RJ"
PlaceInput = Union[int, str, Tuple[str, str]]

# Common Literals
CensusTheme = Literal["basic", "income", "age", "race"]
GeoGranularity = Literal["tract", "municipality", "h3"]