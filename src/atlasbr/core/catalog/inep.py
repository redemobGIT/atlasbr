"""
AtlasBR - Core Catalog for Schools (INEP) Data.
"""

from __future__ import annotations

from typing import Literal, Optional
from warnings import warn

from pydantic import BaseModel, ConfigDict

SchoolsStrategy = Literal["bd"]


class SchoolsThemeSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy: SchoolsStrategy
    table_directory: str = "basedosdados.br_bd_diretorios_brasil.escola"
    table_census: str = "basedosdados.br_inep_censo_escolar.escola"
    year: Optional[int] = None # TODO: given that the catalog is time-agnostic for BD, should we keep year here?


def get_schools_spec(year: int) -> SchoolsThemeSpec:
    warn(
        "INEP Schools catalog is time-agnostic for BD; pass year to "
        "load/fetch functions for filtering.",
        DeprecationWarning,
        stacklevel=2,
    )
    return SchoolsThemeSpec(strategy="bd", year=year)
