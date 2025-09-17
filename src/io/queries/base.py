from __future__ import annotations
from typing import Mapping, Sequence

def sql_select(table: str, columns: Sequence[str]) -> str:
    cols = ", ".join(columns)
    return f"SELECT {cols} FROM `{table}`"

def sql_where_and(filters: Mapping[str, object] | None) -> str:
    if not filters:
        return ""
    parts = []
    for col, val in filters.items():
        if isinstance(val, (list, tuple, set)):
            vals = ", ".join(repr(v) for v in val)
            parts.append(f"{col} IN ({vals})")
        else:
            parts.append(f"{col} = {repr(val)}")
    return "WHERE " + " AND ".join(parts)

def sql_limit(n: int | None) -> str:
    return f"LIMIT {n}" if n is not None else ""

def sql_compose(select: str, where: str = "", limit: str = "") -> str:
    return " ".join(p for p in [select, where, limit] if p)
