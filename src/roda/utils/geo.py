import pandas as pd
import geopandas as gpd
from typing import Mapping, Sequence, Tuple


def geometry_from_wkt(
    df: pd.DataFrame,
    *,
    wkt_col: str = "centroide",
    crs: str | int = "EPSG:4326",
) -> gpd.GeoDataFrame:
    """Converte uma coluna WKT em geometria e retorna um GeoDataFrame."""
    geometry = gpd.GeoSeries.from_wkt(df[wkt_col], crs=crs)
    return gpd.GeoDataFrame(
        df, geometry=geometry, crs=crs
    ).drop(columns=wkt_col)


def to_local_utm(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reprojeta um GeoDataFrame para o fuso UTM local apropriado."""
    # O datum SIRGAS 2000 é o padrão para o Brasil
    utm_crs = gdf.estimate_utm_crs(datum_name="SIRGAS 2000")
    return gdf.to_crs(utm_crs)


def override_geometry(
    gdf: gpd.GeoDataFrame,
    overrides: Mapping[str, Tuple[float, float]],
    *,
    id_column: str | None = None,
    crs_in: str = "EPSG:4326",
    crs_out: str = "EPSG:31983",
    drop_ids: Sequence[str] | None = None
) -> gpd.GeoDataFrame:
    """
    Substitui geometrias de um GeoDataFrame segundo um mapa de overrides.

    Parâmetros
    ----------
    gdf : GeoDataFrame original em crs_in
    overrides : mapeamento {id: (lat, lon)} (EPSG:4326)
    id_column : coluna usada como índice temporário (str ou None)
    crs_in : CRS de entrada (p.ex. 'EPSG:4326')
    crs_out: CRS de saída (p.ex. 'EPSG:31983')
    drop_ids : lista de índices a remover antes de aplicar overrides

    Retorna
    -------
    GeoDataFrame reprojetado em crs_out, com geometrias sobrescritas.
    """
    # reprojeta e copia
    working = gdf.to_crs(crs_out).copy()

    # descarta índices inválidos, se houver
    if drop_ids:
        working = working.drop(index=drop_ids, errors="ignore")

    # indexação temporária por id_column, se fornecido
    if id_column:
        working = working.set_index(working[id_column].astype(str), drop=False)

    # constrói GeoDataFrame de overrides e reprojeta
    df_ovr = pd.DataFrame.from_dict(
        {idx: {"lon": lonlat[1], "lat": lonlat[0]}
         for idx, lonlat in overrides.items()},
        orient="index"
    )
    gdf_ovr = (
        gpd.GeoDataFrame(
            df_ovr,
            geometry=gpd.points_from_xy(df_ovr.lon, df_ovr.lat),
            crs=crs_in
        )
        .to_crs(crs_out)
    )

    # aplica a substituição pontual
    working.loc[gdf_ovr.index, "geometry"] = gdf_ovr.geometry

    # restaura índice simples, se usamos id_column
    if id_column:
        working = working.reset_index(drop=True)

    return working
