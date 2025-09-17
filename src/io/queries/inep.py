from __future__ import annotations
from typing import Iterable, Mapping, Optional
from .base import sql_select, sql_where_and, sql_limit, sql_compose
from .common import INEP_ESCOLAS_COLS, INEP_TURMAS_COLS

_ESCOLAS_TABLE = "basedosdados.br_inep_censo_escolar.escola"
_TURMAS_TABLE = "basedosdados.br_inep_censo_escolar.turma"

def query_inep_escolas(ano: int, cols: Optional[Iterable[str]] = None, filters: Optional[Mapping[str, object]] = None,
                       limit: Optional[int] = None) -> str:
    sel = sql_select(_ESCOLAS_TABLE, columns=list(cols) if cols else INEP_ESCOLAS_COLS)
    eff_filters = dict(filters or {})
    eff_filters["ano"] = ano
    where = sql_where_and(eff_filters)
    lim = sql_limit(limit)
    return sql_compose(sel, where, lim)

def query_inep_turmas(ano: int, cols: Optional[Iterable[str]] = None, filters: Optional[Mapping[str, object]] = None,
                      limit: Optional[int] = None) -> str:
    sel = sql_select(_TURMAS_TABLE, columns=list(cols) if cols else INEP_TURMAS_COLS)
    eff_filters = dict(filters or {})
    eff_filters["ano"] = ano
    where = sql_where_and(eff_filters)
    lim = sql_limit(limit)
    return sql_compose(sel, where, lim)
