from __future__ import annotations
from typing import Iterable, Mapping, Optional
from .base import sql_select, sql_where_and, sql_limit, sql_compose
from .common import CENSO_SETOR_COLS, CENSO_POP_COLS

_SETOR_TABLE = "basedosdados.br_ibge_censo.setor_censitario"
_POP_TABLE = "basedosdados.br_ibge_censo.populacao"

def query_censo_setores(ano: int, cols: Optional[Iterable[str]] = None, filters: Optional[Mapping[str, object]] = None,
                        limit: Optional[int] = None) -> str:
    sel = sql_select(_SETOR_TABLE, columns=list(cols) if cols else CENSO_SETOR_COLS)
    eff_filters = dict(filters or {})
    eff_filters["ano"] = ano
    where = sql_where_and(eff_filters)
    lim = sql_limit(limit)
    return sql_compose(sel, where, lim)

def query_censo_populacao(ano: int, cols: Optional[Iterable[str]] = None, filters: Optional[Mapping[str, object]] = None,
                          limit: Optional[int] = None) -> str:
    sel = sql_select(_POP_TABLE, columns=list(cols) if cols else CENSO_POP_COLS)
    eff_filters = dict(filters or {})
    eff_filters["ano"] = ano
    where = sql_where_and(eff_filters)
    lim = sql_limit(limit)
    return sql_compose(sel, where, lim)
