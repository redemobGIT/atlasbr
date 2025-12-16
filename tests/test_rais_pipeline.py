import pytest
from unittest.mock import patch
import pandas as pd
from atlasbr.app.rais import load_rais

def test_load_rais_basic(mock_rais_df):
    """
    Smoke test for load_rais pipeline (no geocoding).
    """
    with patch("atlasbr.infra.adapters.rais_bd.fetch_rais_from_bd", return_value=mock_rais_df), \
         patch("atlasbr.infra.geo.resolver.resolve_places_to_ids", return_value=[1234567]):
         
        df = load_rais(
            places=["Mock City"],
            year=2022,
            strategy="bd_table",
            gcp_billing="test-project",
            geocode=False
        )
        
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        # Check basic cleaning (logic.filter_invalid_legal_nature)
        assert len(df) > 0