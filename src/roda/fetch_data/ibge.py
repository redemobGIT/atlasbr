import pandas as pd

from roda.defaults.ibge import POP_PROJECTION_URL


def read_pop_projections() -> pd.DataFrame:
    """
    Download and process population projections data from IBGE.

    Returns:
        pd.DataFrame: A DataFrame with columns 'id_setor_censitario' and
                      'rendimento_medio'.
    """
    return pd.read_excel(
        POP_PROJECTION_URL,
        skiprows=6,
        usecols="A:G,N:P,Z:AB,AF:AH",
    )
