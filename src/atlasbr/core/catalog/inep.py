"""
AtlasBR - Core Catalog for Schools (INEP) Data.
"""

from typing import Literal
from pydantic import BaseModel, ConfigDict

class SchoolsThemeSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    
    year: int
    strategy: Literal["bd_complex_sql"]
    
    # Table references
    table_directory: str = "basedosdados.br_bd_diretorios_brasil.escola"
    table_census: str = "basedosdados.br_inep_censo_escolar.escola"

def get_schools_spec(year: int) -> SchoolsThemeSpec:
    return SchoolsThemeSpec(year=year, strategy="bd_complex_sql")