from typing import Sequence, Any

def sql_list(values: Sequence[Any], *, quote: bool = False, pad: int | None = None) -> str:
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