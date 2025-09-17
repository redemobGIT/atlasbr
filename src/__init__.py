# urbikit/__init__.py
"""Urbikit — kit enxuto para dados urbanos.

Atalhos no nível do pacote:
- load_bd_sql, load_bdd_table, load_http, load_ftp, load_file

Submódulos principais:
- io, adapters, clean, transform, spatial, viz, utils
- queries (como alias de urbikit.io.queries)
"""

from __future__ import annotations

# --- Atalhos de I/O expostos no topo do pacote ---
from .io import (  # reexporta funções puras, sem efeitos colaterais na importação
    load_bd_sql,
    load_bdd_table,
    load_http,
    load_ftp,
    load_file,
)

# --- Submódulos úteis mantidos explícitos ---
from . import io, adapters, clean, transform, spatial, viz, utils

# Alias conveniente para o namespace de consultas SQL reutilizáveis
from .io import queries as queries  # noqa: F401

__all__ = [
    # atalhos de I/O
    "load_bd_sql",
    "load_bdd_table",
    "load_http",
    "load_ftp",
    "load_file",
    # submódulos
    "io",
    "adapters",
    "clean",
    "transform",
    "spatial",
    "viz",
    "utils",
    # alias de consultas
    "queries",
]

__version__ = "0.1.0"
