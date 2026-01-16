__version__ = "0.1.0"

import logging

from .settings import configure_logging, get_billing_id, set_billing_id

__all__ = [
    "configure_logging",
    "set_billing_id",
    "get_billing_id",
    "load_census",
    "load_rais",
    "load_cnes",
    "load_schools",
    "plot_distribution",
    "plot_point_heatmap",
    "describe_numeric",
    "describe_grouped",
]

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
    if name == "plot_distribution":
        from .viz import plot_distribution

        return plot_distribution
    if name == "plot_point_heatmap":
        from .viz import plot_point_heatmap

        return plot_point_heatmap
    if name == "describe_numeric":
        from .viz import describe_numeric

        return describe_numeric
    if name == "describe_grouped":
        from .viz import describe_grouped

        return describe_grouped
    raise AttributeError(f"module 'atlasbr' has no attribute {name}")


def __dir__():
    return sorted(list(globals().keys()) + __all__)
