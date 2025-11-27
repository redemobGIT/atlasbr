from .census import load_census
from .rais import load_rais
from .cnes import load_cnes
from .schools import load_schools

__all__ = [
    "load_census",
    "load_rais",
    "load_cnes",
    "load_schools",
]