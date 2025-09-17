from __future__ import annotations
import pandas as pd

def load_bdd_table(dataset_id: str, table_id: str, *, project: str | None = None, **kwargs) -> pd.DataFrame:
    """Lê tabela via cliente oficial `basedosdados.read_table`."""
    try:
        import basedosdados as bd  # type: ignore
    except Exception as exc:
        raise RuntimeError("basedosdados não instalado. Use `pip install basedosdados`.") from exc
    return bd.read_table(dataset_id=dataset_id, table_id=table_id, billing_project_id=project, **kwargs)
