import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import geopandas as gpd
from atlasbr.app.census import load_census

def test_load_census_signature_defaults(mock_tracts_gdf, mock_urban_gdf, mock_census_df):
    """
    Smoke test for load_census to ensure it runs with mocked data and default args.
    """
    with patch("atlasbr.infra.geo.tracts.fetch_tracts_raw", return_value=mock_tracts_gdf), \
         patch("atlasbr.infra.geo.footprint.fetch_urban_area_raw_gdf", return_value=mock_urban_gdf), \
         patch("atlasbr.infra.adapters.census_bd.fetch_from_bd", return_value=mock_census_df), \
         patch("atlasbr.infra.geo.resolver.resolve_places_to_ids", return_value=[1234567]):
         
        # Test BD strategy
        gdf = load_census(
            places=["Mock City"],
            strategy="bd_table",
            themes=["basic"],
            gcp_billing="test-project"
        )
        
        assert isinstance(gdf, gpd.GeoDataFrame)
        assert not gdf.empty
        assert "geometry" in gdf.columns
        # Check if join worked (mock_census_df cols should be present)
        assert "moradores_particulares_permanentes" in gdf.columns

def test_load_census_ftp_strategy(mock_tracts_gdf, mock_urban_gdf, mock_census_df):
    """
    Smoke test for load_census with FTP strategy (no billing required).
    """
    with patch("atlasbr.infra.geo.tracts.fetch_tracts_raw", return_value=mock_tracts_gdf), \
         patch("atlasbr.infra.geo.footprint.fetch_urban_area_raw_gdf", return_value=mock_urban_gdf), \
         patch("atlasbr.infra.adapters.census_ftp.fetch_census_ftp", return_value=mock_census_df), \
         patch("atlasbr.infra.geo.resolver.resolve_places_to_ids", return_value=[1234567]):
         
        gdf = load_census(
            places=["Mock City"],
            strategy="ftp_csv",
            themes=["basic"]
        )
        
        assert isinstance(gdf, gpd.GeoDataFrame)
        assert not gdf.empty