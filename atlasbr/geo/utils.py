"""
AtlasBR - Geo Utilities.
"""
import geopandas as gpd
import pandas as pd
from typing import Mapping, Tuple, Sequence

def to_local_utm(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reprojeta um GeoDataFrame para o fuso UTM local apropriado (SIRGAS 2000)."""
    if gdf.empty or gdf.geometry.isnull().all():
         return gdf
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


def prepare_geodata(
    gdf: gpd.GeoDataFrame, id_col: str, year_col: str, vars_to_show: List[str]
) -> Tuple[gpd.GeoDataFrame, Dict, List[str], List[int]]:
    """
    Prepares the GeoDataFrame and extracts the base GeoJSON for the map.
    """
    cols_needed = [year_col, id_col, "geometry", *vars_to_show]
    gdf = gdf[cols_needed].dropna(subset=[id_col, "geometry"]).copy()
    gdf = gdf.to_crs(4326)
    gdf[id_col] = gdf[id_col].astype(str)

    # Ensure consistent integer-like year ordering even if dtype is object
    years = sorted(pd.unique(gdf[year_col].astype(int)).tolist())

    geo_base = (
        gdf[[id_col, "geometry"]]
        .drop_duplicates(subset=[id_col])
        .set_index(id_col)
        .sort_index()
    )

    # Keep the ID as a property in the GeoJSON
    geojson = json.loads(geo_base.reset_index().to_json())
    locs = geo_base.index.astype(str).tolist()

    return gdf, geojson, locs, years