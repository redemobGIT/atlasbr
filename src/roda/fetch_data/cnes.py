import pandas as pd
from typing import Sequence, Optional
import basedosdados as bd

from roda.utils.sql import sql_list, load_query
from roda.defaults.cnes import CNES_INFRASTRUCTURE_GROUPS, CNES_UNIT_CODES


# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================


def _group_expressions() -> str:
    """
    Monta expressões SQL com COALESCE() para cada grupo de infraestrutura CNES.
    """
    out: list[str] = []
    for alias, cols in CNES_INFRASTRUCTURE_GROUPS.items():
        summands = " + ".join(f"COALESCE({c}, 0)" for c in cols)
        out.append(f"{summands} AS {alias}")
    return ",\n                      ".join(out)


# =============================================================================
# FUNÇÃO PRINCIPAL DE COLETA
# =============================================================================


def fetch_healthcare(
    muni_ids: Sequence[int | str],
    *,
    ano: int = 2023,
    mes: int = 9,
    billing_project_id: Optional[str] = None,
    query: Optional[str] = None,
) -> pd.DataFrame:
    """
    Busca estabelecimentos CNES enriquecidos com somas de infraestrutura.
    """
    muni_sql = sql_list(muni_ids, quote=True, pad=7)
    unit_sql = sql_list(CNES_UNIT_CODES, quote=True)
    infra_sql = _group_expressions()

    if query is None:
        query = load_query(
            "fetch_healthcare.sql",
            muni_sql=muni_sql,
            unit_sql=unit_sql,
            infra_sql=infra_sql,
            ano=ano,
            mes=mes,
        )
    else:
        query = query.format(
            muni_sql=muni_sql,
            unit_sql=unit_sql,
            infra_sql=infra_sql,
            ano=ano,
            mes=mes,
        )

    return bd.read_sql(query, billing_project_id=billing_project_id)
