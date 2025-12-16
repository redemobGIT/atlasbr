__version__ = "0.1.0"

from .settings import configure_logging, set_billing_id, get_billing_id

__all__ = [
    "configure_logging",
    "set_billing_id",
    "get_billing_id",
    "load_census",
    "load_rais",
    "load_cnes",
    "load_schools",
]

import logging
logging.getLogger("atlasbr").addHandler(logging.NullHandler())

def __getattr__(name: str):
    if name == "load_census":
        from .app.census import load_census
        return load_census
    if name == "load_rais":
        from .app.rais import load_rais
        return load_rais
    if name == "load_cnes":
        from .app.cnes import load_cnes
        return load_cnes
    if name == "load_schools":
        from .app.inep import load_schools
        return load_schools
    raise AttributeError(f"module 'atlasbr' has no attribute {name}")

def __dir__():
    return sorted(list(globals().keys()) + __all__)
