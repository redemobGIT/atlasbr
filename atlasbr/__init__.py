__version__ = "0.1.0"

# 1. Expose Settings & Configuration helpers
from .settings import configure_logging, set_billing_id, get_billing_id

# 2. Expose Main Application Pipelines (The Public API)
# This allows: atlasbr.load_census(...)
from .app.census import load_census
from .app.rais import load_rais
from .app.cnes import load_cnes
from .app.inep import load_schools

# 3. Expose Visualization (Optional, keeps it handy)
# from . import viz

# Define what happens on 'from atlasbr import *'
__all__ = [
    "configure_logging",
    "set_billing_id",
    "get_billing_id",
    "load_census",
    "load_rais",
    "load_cnes",
    "load_schools",
]

# Optional: Auto-configure default logging to avoid "No handler found" warnings
import logging
logging.getLogger("atlasbr").addHandler(logging.NullHandler())