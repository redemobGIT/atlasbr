from __future__ import annotations
from typing import Iterable, Mapping, Optional
from .base import sql_select, sql_where_and, sql_limit, sql_compose
from .common import CNES_ESTAB_COLS, CNES_PROF_COLS

_ESTAB_TABLE = "basedosdados.br_ms_cnes.estabelecimento"
_PROF_TABLE = "basedosdados.br_ms_cnes.profissional"

def query_cnes_estabelecimentos(ano: int, cols: Optional[Iterable[str]] = None, filters: Optional[Mapping[str, object]] = None,
                                limit: Optional[int] = None) -> str:
    sel = sql_select(_ESTAB_TABLE, columns=list(cols) if cols else CNES_ESTAB_COLS)
    eff_filters = dict(filters or {})
    eff_filters["ano"] = ano
    where = sql_where_and(eff_filters)
    lim = sql_limit(limit)
    return sql_compose(sel, where, lim)

def query_cnes_profissionais(ano: int, cols: Optional[Iterable[str]] = None, filters: Optional[Mapping[str, object]] = None,
                             limit: Optional[int] = None) -> str:
    sel = sql_select(_PROF_TABLE, columns=list(cols) if cols else CNES_PROF_COLS)
    eff_filters = dict(filters or {})
    eff_filters["ano"] = ano
    where = sql_where_and(eff_filters)
    lim = sql_limit(limit)
    return sql_compose(sel, where, lim)
