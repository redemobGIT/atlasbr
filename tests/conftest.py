import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon

@pytest.fixture
def mock_tracts_gdf():
    """Returns a minimal GeoDataFrame resembling raw tracts."""
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    df = pd.DataFrame({
        "id_setor_censitario": ["12345670000001", "12345670000002"],
        "geometry": [poly, poly]
    })
    return gpd.GeoDataFrame(df, crs="EPSG:4674")

@pytest.fixture
def mock_urban_gdf():
    """Returns a minimal Urban Footprint GeoDataFrame."""
    poly = Polygon([(-1, -1), (2, -1), (2, 2), (-1, 2)]) # Covers the tracts
    df = pd.DataFrame({"geometry": [poly]})
    return gpd.GeoDataFrame(df, crs="EPSG:4674")

@pytest.fixture
def mock_census_df():
    """Returns a minimal Census attribute DataFrame."""
    df = pd.DataFrame({
        "id_setor_censitario": ["12345670000001", "12345670000002"],
        "domicilios_particulares_permanentes": [100, 200],
        "moradores_particulares_permanentes": [300, 600],
        "rendimento_medio": [1500.0, 2500.0]
    })
    return df.set_index("id_setor_censitario")

@pytest.fixture
def mock_rais_df():
    """Returns a minimal RAIS DataFrame."""
    return pd.DataFrame({
        "id_municipio": ["1234567", "1234567"],
        "cnae_2": ["12345", "67890"],
        "vinculo_ativo_3112": [1, 1],
        "quantidade_vinculos": [10, 5], 
        "cep_estab": ["20000000", "20000001"],
        "natureza_juridica": ["2062", "2062"]
    })