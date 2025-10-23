from fileinput import filename
from typing import Sequence, Any, Optional
from pathlib import Path


def sql_list(
    values: Sequence[Any], *, quote: bool = False, pad: int | None = None
) -> str:
    """
    Converte uma sequência de valores para uma lista literal em SQL.
    """
    vals = list(values)
    if not vals:
        raise ValueError("A sequência não pode ser vazia.")

    def _fmt(v: Any) -> str:
        s = str(int(v) if isinstance(v, (int, float)) else v)
        if pad is not None:
            s = s.zfill(pad)
        return f"'{s}'" if quote else s

    return ", ".join(_fmt(v) for v in vals)


def _format_query(query: str, params: dict[str, Any]) -> str:
    """Formata uma query string usando os parâmetros fornecidos."""
    try:
        return query.format(**params)
    except KeyError as e:
        raise ValueError(f"O parâmetro {e} é necessário mas não foi fornecido.")


def format_query_from_file(filepath: str, **params: Optional[Any]) -> str:
    """Carrega uma query a partir de um arquivo e a formata com os parâmetros fornecidos."""
    if not filepath:
        raise ValueError("O caminho do arquivo (filepath) deve ser fornecido.")

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            query = f.read()
    except FileNotFoundError:
        raise FileNotFoundError(
            f"O arquivo especificado em '{filepath}' não foi encontrado."
        )
    except IOError as e:
        raise IOError(f"Não foi possível ler o arquivo em '{filepath}': {e}")

    return _format_query(query, params)


def format_query_from_string(query_string: str, **params: Optional[Any]) -> str:
    """Formata uma string de query com os parâmetros fornecidos."""
    if not isinstance(query_string, str) or not query_string.strip():
        raise ValueError("A 'query_string' não pode ser vazia ou nula.")

    return _format_query(query_string, params)
