from typing import Sequence
import pandas as pd
import basedosdados as bd

from roda.utils.sql import sql_list

def load_ceps(
    municipalities_ids: Sequence[int | str],
    *,
    billing_project_id: str | None = None,
) -> pd.DataFrame:
    """Retorna um DataFrame bruto com todos os CEPs para os municípios.

    A coluna de geometria é retornada como texto (WKT).

    Args:
        municipalities_ids (Sequence[int | str]): Lista de códigos IBGE de
            7 dígitos dos municípios.
        billing_project_id (str | None, optional): ID do projeto no Google Cloud
            para faturamento da query. Defaults to None.

    Returns:
        pd.DataFrame: Tabela de CEPs como um DataFrame do Pandas.
    """
    municipality_sql = sql_list(municipalities_ids, quote=True, pad=7)
    query = f"""
        SELECT *
        FROM `basedosdados.br_bd_diretorios_brasil.cep`
        WHERE id_municipio IN ({municipality_sql})
    """
    return bd.read_sql(query, billing_project_id=billing_project_id)