import geopandas as gpd

from typing import Sequence

from roda.fetch_data.ceps import load_ceps
from roda.fetch_data.inep import fetch_schools
from roda.fetch_data.cnes import fetch_healthcare
from roda.fetch_data.rais import fetch_rais
from roda.processing.geocoding import merge_by_cep
from roda.utils.geo import geometry_from_wkt, to_local_utm


# OLHAR COM MAIS CALMA
# |-> Pode ser que o municipalities passado no notebook rais não esteja condizente com o esperado
# Nesse caso, devo avaliar a lógica da implementação conjuntamente

def load_geolocated_layers(
    municipalities: Sequence[int | str],
    *,
    year: int,
    cnes_month: int,
    billing_project_id: str
) -> dict[str, gpd.GeoDataFrame]:
    """
    Carrega, georreferencia e retorna as camadas de dados RAIS, CNES e Escolas.
    
    Esta função orquestra todo o pipeline de busca e preparação dos dados,
    devolvendo um dicionário de GeoDataFrames prontos para análise em UTM local.
    """
    # 1. Base para geocodificação
    ceps_raw = load_ceps(municipalities, billing_project_id=billing_project_id)
    ceps_gdf = geometry_from_wkt(ceps_raw)

    # 2. Camada de Escolas (já vem com lat/lon)
    schools_raw = fetch_schools(municipalities, year_censo=year, billing_project_id=billing_project_id)
    schools_gdf = gpd.GeoDataFrame(
        schools_raw,
        geometry=gpd.points_from_xy(schools_raw.longitude, schools_raw.latitude),
        crs="EPSG:4326",
    ).pipe(to_local_utm)

    # 3. Camada do CNES (geocodificação via CEP)
    cnes_raw = fetch_healthcare(
        municipalities, ano=year, mes=cnes_month, billing_project_id=billing_project_id
    )
    cnes_gdf = merge_by_cep(cnes_raw, ceps_gdf).pipe(to_local_utm)

    # 4. Camada da RAIS (geocodificação via CEP)
    rais_raw = fetch_rais(municipalities, years=year, billing_project_id=billing_project_id)
    rais_gdf = merge_by_cep(rais_raw, ceps_gdf).pipe(to_local_utm)

    return {"rais": rais_gdf, "cnes": cnes_gdf, "schools": schools_gdf}