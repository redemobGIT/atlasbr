import geopandas as gpd
from tobler.area_weighted import area_interpolate

def aggregate_to_hex(
    src_gdf: gpd.GeoDataFrame,
    hex_gdf: gpd.GeoDataFrame,
    extensive: list[str],
    intensive: list[str] | None = None,
) -> gpd.GeoDataFrame:
    """
    Interpola variáveis de setores censitários para uma grade hexagonal H3
    usando pesos de área.
    """
    interpolated = area_interpolate(
        source_df=src_gdf,
        target_df=hex_gdf,
        extensive_variables=extensive,
        intensive_variables=intensive,
        allocate_total=True,
    ).drop(columns="geometry")

    out = hex_gdf.join(interpolated)
    return out