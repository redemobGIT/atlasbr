from __future__ import annotations
from typing import Iterable, Mapping, Optional, Sequence, Dict, Any
from .base import sql_select, sql_where_and, sql_limit, sql_compose
from .common import RAIS_VINC_COLS, RAIS_ESTAB_COLS

_VINC_TABLE = "basedosdados.br_me_rais.vinculos"
_ESTAB_TABLE = "basedosdados.br_me_rais.estabelecimentos"

def _merge_filters(filters: Optional[Mapping[str, object]], years: Optional[Sequence[int]]) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(filters or {})
    if years is not None and "ano" not in out:
        if isinstance(years, int):
            out["ano"] = [years]
        else:
            out["ano"] = list(years)
    return out

def query_rais_vinculos(*, cols: Optional[Iterable[str]] = None, filters: Optional[Mapping[str, object]] = None,
                        years: Optional[Sequence[int] | int] = None, limit: Optional[int] = None) -> str:
    eff_filters = _merge_filters(filters, years)
    sel = sql_select(_VINC_TABLE, columns=list(cols) if cols else RAIS_VINC_COLS)
    where = sql_where_and(eff_filters)
    lim = sql_limit(limit)
    return sql_compose(sel, where, lim)

def query_rais_estabelecimentos(*, cols: Optional[Iterable[str]] = None, filters: Optional[Mapping[str, object]] = None,
                                years: Optional[Sequence[int] | int] = None, limit: Optional[int] = None) -> str:
    eff_filters = _merge_filters(filters, years)
    sel = sql_select(_ESTAB_TABLE, columns=list(cols) if cols else RAIS_ESTAB_COLS)
    where = sql_where_and(eff_filters)
    lim = sql_limit(limit)
    return sql_compose(sel, where, lim)
