from pathlib import Path

_SQL_PATH = Path(__file__).resolve().parent.parent / "defaults" / "queries"

FETCH_RAIS_SQL_PATH = _SQL_PATH / "fetch_rais.sql"
FETCH_SCHOOLS_SQL_PATH = _SQL_PATH / "fetch_schools.sql"
FETCH_HEALTHCARE_SQL_PATH = _SQL_PATH / "fetch_healthcare.sql"
