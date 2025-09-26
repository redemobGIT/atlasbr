import geopandas as gpd
import pandas as pd
from typing import Optional
from .census_mapping import CFG

# --- Constantes de URLs para Download de Dados do IBGE ---

_IBGE_AREAS_URBANIZADAS_BASE_URL = (
    "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
    "tipologias_do_territorio/areas_urbanizadas_do_brasil"
)

# Mapeia o ano de referência para o arquivo de download correspondente
URL_URBAN_AREAS = {
    2005: "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2005/areas_urbanizadas_do_Brasil_2005_shapes.zip",
    2015: "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2015/Shape/AreasUrbanizadasDoBrasil_2015.zip",
    2019: "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2019/Shapefile/AreasUrbanizadas2019_Brasil.zip"
}

URL_INCOME_CENSUS__2022 = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Agregados_por_Setores_Censitarios_Rendimento_do_Responsavel/"
    "Agregados_por_setores_renda_responsavel_BR_csv.zip"
)













# --- Classes e Funções de Coleta (como a classe CensusGrid) ---

class CensusGrid:
    """
    Representa e gerencia os dados da grade estatística do Censo do IBGE.
    """
    def __init__(self, year: int, state: str):
        self.year = year
        self.state = state
        self.gdf: gpd.GeoDataFrame | None = None

    def load_data(self):
        """
        Baixa e carrega os dados da grade para o ano e estado especificados.
        """
        # ... lógica de download que hoje está em uma função ...
        print(f"Carregando dados do Censo {self.year} para {self.state}...")
        # self.gdf = ...
        return self

    def calculate_person_density(self, area_km2_col: str) -> 'CensusGrid':
        """
        Calcula a densidade de pessoas por km².
        """
        if self.gdf is None:
            raise ValueError("Dados não carregados. Chame .load_data() primeiro.")
        
        self.gdf['densidade_pop'] = self.gdf['populacao_total'] / self.gdf[area_km2_col]
        return self

# --- Funções auxiliares (se houver necessidade de funções fora da classe) ---
def get_municipality_boundary(municipality_name: str, state_uf: str) -> gpd.GeoDataFrame:
    """
    Função para baixar ou carregar o limite de um município.
    Placeholder para a lógica real.
    """
    print(f"Obtendo limite de {municipality_name}, {state_uf}...")
    # Exemplo Simulado:
    # Cria um polígono dummy para o Rio de Janeiro
    if municipality_name == 'Rio de Janeiro' and state_uf == 'RJ':
        from shapely.geometry import Polygon
        poly = Polygon([
            (-43.5, -23.0), (-43.5, -22.7), (-43.1, -22.7), (-43.1, -23.0), (-43.5, -23.0)
        ])
        return gpd.GeoDataFrame([{'name': municipality_name, 'geometry': poly}], crs="EPSG:4326")
    else:
        raise NotImplementedError("Apenas o limite do Rio de Janeiro (RJ) está simulado.")
