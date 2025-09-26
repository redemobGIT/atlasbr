import pandas as pd
import geopandas as gpd

def merge_by_cep(
    df: pd.DataFrame,
    cep_gdf: gpd.GeoDataFrame,
    *,
    cep_col: str = "cep"
) -> gpd.GeoDataFrame:
    """Une um DataFrame a geometrias de CEP, mantendo apenas os matches.

    Esta função enriquece um DataFrame (ex: da RAIS) com geometrias
    ao uni-lo com um GeoDataFrame de CEPs.

    Args:
        df (pd.DataFrame): DataFrame a ser geocodificado, contendo `cep_col`.
        cep_gdf (gpd.GeoDataFrame): GeoDataFrame de referência dos CEPs,
            contendo `cep_col` e uma coluna 'geometry'.
        cep_col (str, optional): Nome da coluna de CEP em ambos os DataFrames.
            Defaults to "cep".

    Returns:
        gpd.GeoDataFrame: Um novo GeoDataFrame contendo apenas as linhas de `df`
            cujo CEP foi encontrado em `cep_gdf`, agora com a coluna de geometria.
    """
    df_copy = df.copy()
    # Garante que a coluna CEP tenha o formato correto para o merge (string com 8 dígitos)
    df_copy[cep_col] = df_copy[cep_col].astype(str).str.zfill(8)
    
    # Seleciona apenas as colunas necessárias do GeoDataFrame de CEPs
    cep_ref_gdf = cep_gdf[[cep_col, "geometry"]].copy()
    cep_ref_gdf[cep_col] = cep_ref_gdf[cep_col].astype(str).str.zfill(8)

    # Une os dois DataFrames
    merged_gdf = pd.merge(df_copy, cep_ref_gdf, on=cep_col, how="inner")
    
    # Garante que o resultado final seja um GeoDataFrame com o CRS correto
    return gpd.GeoDataFrame(merged_gdf, geometry="geometry", crs=cep_gdf.crs)