from typing import Iterable
import pandas as pd
import geopandas as gpd
import basedosdados as bd
from functools import lru_cache
import zipfile
from io import BytesIO
import requests
import numpy as np
import geobr

from .urban_area import get_urban_area_gdf

# =============================================================================
# MAPEAMENTOS E CONSTANTES INTERNAS
# =============================================================================

# Dicionário de mapeamento interno para as fontes de dados do Censo
_CENSO_DATA_MAP = {
    "basic": {
        2010: dict(
            table="basedosdados.br_ibge_censo_demografico.setor_censitario_basico_2010",
            cols=dict(v002="habitantes", v001="domicilios"),
        ),
        2022: dict(
            table="basedosdados.br_ibge_censo_2022.setor_censitario",
            cols=dict(pessoas="habitantes", domicilios="domicilios"),
        ),
    },
    "income": {
        2010: dict(
            table="basedosdados.br_ibge_censo_demografico.setor_censitario_basico_2010",
            cols=dict(v009="rendimento_medio"),
        ),
        2022: dict(
            external="_income_2022",
        ),
    },
    "age": {
        2010: {"external": "_age_2010"},
        2022: {"external": "_age_2022"},
    },
    "race": {
        2010: {"external": "_race_2010"},
        2022: {"external": "_race_2022"},
    },
}

_BD_TABLE_SETOR_2022 = "basedosdados.br_ibge_censo_2022.setor_censitario"
_CENSO_RACES = ("branca", "preta", "amarela", "parda", "indigena")
_INCOME_2022_URL = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Agregados_por_Setores_Censitarios_Rendimento_do_Responsavel/"
    "Agregados_por_setores_renda_responsavel_BR_csv.zip"
)

# =============================================================================
# FUNÇÃO DE INTERFACE PÚBLICA
# =============================================================================

def import_census_data(
    muni_codes: Iterable[int],
    *,
    gcp_billing: str,
    themes: tuple[str] = ("basic", "income"),
    clip_urban: bool = True,
) -> gpd.GeoDataFrame:
    """
    Função principal para importar e montar dados do Censo para 2010 e 2022.

    Orquestra a busca de geometrias (setores censitários) e a junção de
    diversos temas de atributos (população, renda, etc.) para os anos e
    municípios especificados.

    Args:
        muni_codes (Iterable[int]): Códigos IBGE de 7 dígitos dos municípios.
        gcp_billing (str): ID do projeto no Google Cloud para faturamento do BigQuery.
        themes (tuple[str], optional): Temas a serem baixados.
            Defaults to ("basic", "income").
        clip_urban (bool, optional): Se True, recorta os setores censitários
            para a mancha urbana. Defaults to True.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame com os dados do censo, contendo um
            MultiIndex (id_setor_censitario, year).
    """
    muni_codes = tuple(map(int, np.atleast_1d(muni_codes)))
    frames = [_census_year(y, muni_codes, gcp_billing, themes, clip_urban) for y in (2010, 2022)]
    return (pd.concat(frames)
              .set_index("year", append=True))  # MultiIndex: (tract, year)

# =============================================================================
# FUNÇÕES DE ORQUESTRAÇÃO INTERNAS
# =============================================================================

def _census_year(year: int,
                 munis: tuple[int],
                 billing: str,
                 themes: tuple[str],
                 clip_urban: bool = True) -> gpd.GeoDataFrame:
    """Monta o GeoDataFrame completo para um único ano."""
    gdf = _tracts(munis, year=year, clip_to_urban=clip_urban)
    for t in themes:
        gdf = gdf.join(_query_theme(t, year, munis, billing), how="left")
    gdf["year"] = year
    return gdf


def _query_theme(theme: str, year: int,
                 munis: tuple[int], billing: str) -> pd.DataFrame:
    """Busca um bloco temático de dados para um dado ano, usando o mapa de dados."""
    spec = _CENSO_DATA_MAP[theme][year]
    if "external" in spec:
        fetcher_func = globals()[spec["external"]]
        return fetcher_func(munis, billing)
    return _pull_bq(spec["table"], spec["cols"], munis, billing)

# =============================================================================
# FUNÇÕES "FETCHER" E DE GEOMETRIA INTERNAS
# =============================================================================

def _pull_bq(table: str,
             cols: dict[str, str],
             munis: Iterable[int],
             billing: str) -> pd.DataFrame:
    """Busca colunas de uma tabela do BigQuery para os municípios especificados."""
    munis_sql = ", ".join(f"'{int(m):07d}'" for m in munis)
    select_sql = ", ".join(f"{raw} AS {alias}" for raw, alias in cols.items())

    sql = f"""
        SELECT id_setor_censitario, {select_sql}
        FROM {table}
        WHERE SUBSTR(id_setor_censitario, 1, 7) IN ({munis_sql})
    """
    return (bd.read_sql(sql, billing_project_id=billing)
              .set_index("id_setor_censitario"))


def _tracts(
    munis: Iterable[int],
    *,
    year: int,
    clip_to_urban: bool = True,
) -> gpd.GeoDataFrame:
    """Retorna os setores censitários (geometrias) usando a biblioteca geobr."""
    munis = np.atleast_1d(munis).astype(int)
    
    gdf = pd.concat(
        geobr.read_census_tract(code_tract=m, year=year, simplified=False)
        for m in munis
    )
    
    crs = gdf.estimate_utm_crs(datum_name="SIRGAS 2000")
    
    gdf = (
        gdf.assign(id_setor_censitario=lambda d:
                   d.code_tract.astype("int64").astype(str).str.zfill(15))
           .set_index("id_setor_censitario")[["geometry"]]
    ).to_crs(crs)

    gdf["geometry"] = gdf.geometry.buffer(0)
    
    print(f"✓ {len(gdf):,} setores censitários carregados para {year}")

    if clip_to_urban:
        mask = _urban_mask(year, str(crs), gdf.total_bounds)
        gdf  = (gpd.overlay(gdf.reset_index(), mask, how="intersection")
                   .set_index("id_setor_censitario"))
        gdf  = gdf[gdf.is_valid & ~gdf.is_empty]

    return gdf


def _urban_mask(year: int, crs: str,
                bbox: tuple[float, float, float, float]) -> gpd.GeoDataFrame:
    """Cria uma máscara de área urbana dissolvida e com buffer."""
    minx, miny, maxx, maxy = bbox
    
    urban = (get_urban_area_gdf(year)
             .to_crs(crs)
             .cx[minx:maxx, miny:maxy]
             .geometry.buffer(0)
             )

    if urban.empty:
        return gpd.GeoDataFrame({"geometry": []}, crs=crs)

    union = urban.unary_union.buffer(500)
    return gpd.GeoDataFrame({"geometry": [union]}, crs=crs)

# =============================================================================
# FUNÇÕES "FETCHER" ESPECIALIZADAS PARA TEMAS EXTERNOS
# =============================================================================

@lru_cache
def _income_2022(*_) -> pd.DataFrame:
    """Busca e processa o rendimento médio do Censo 2022 a partir de um CSV do FTP."""
    with zipfile.ZipFile(BytesIO(requests.get(_INCOME_2022_URL, timeout=60).content)) as zf:
        csv = next(p for p in zf.namelist() if p.endswith(".csv"))
        df = pd.read_csv(
            zf.open(csv),
            sep=";",
            encoding="latin1",
            usecols=["CD_SETOR", "V06004"],
            dtype=str,
        )


    return (
        df.rename(columns={"CD_SETOR": "id_setor_censitario",
                           "V06004": "rendimento_medio"})
          .assign(id_setor_censitario=lambda d: d.id_setor_censitario.str.zfill(15),
                  rendimento_medio=lambda d:
                      pd.to_numeric(d.rendimento_medio.str.replace(",", ".")
                                            .replace({"X": np.nan, ".": np.nan}),
                                      errors="coerce"))
          .set_index("id_setor_censitario")
    )


def _age_2010(muni_ids: tuple[int], billing: str) -> pd.DataFrame:
    """Busca dados de faixa etária do Censo 2010."""
    muni_list = ", ".join(f"'{m}'" for m in map(str, muni_ids))
    sql = f"""
        SELECT
        id_setor_censitario,
        ( IFNULL(v022, 0) + {_sql_sum(range(35, 49), width=3, prefix="IFNULL(v", suffix=", 0)")}) AS age_0_14,
        ({_sql_sum(range(49, 54), width=3, prefix="IFNULL(v", suffix=", 0)")}) AS age_15_19,
        ({_sql_sum(range(54, 99), width=3, prefix="IFNULL(v", suffix=", 0)")}) AS age_20_64,
        ({_sql_sum(range(99, 135), width=3, prefix="IFNULL(v", suffix=", 0)")}) AS age_65p
        FROM `basedosdados.br_ibge_censo_demografico.setor_censitario_idade_total_2010`
        WHERE SUBSTR(id_setor_censitario, 1, 7) IN ({muni_list})
    """
    df = bd.read_sql(sql, billing_project_id=billing)
    return df.set_index('id_setor_censitario').fillna(0)


def _race_2010(muni_ids: tuple[int], billing: str) -> pd.DataFrame:
    """Busca dados de raça/cor do Censo 2010."""
    muni_list = ", ".join(f"'{m}'" for m in map(str, muni_ids))
    sql = f"""
        SELECT
          id_setor_censitario,
          v002 AS race_branca, v003 AS race_preta, v004 AS race_amarela,
          v005 AS race_parda, v006 AS race_indigena
        FROM `basedosdados.br_ibge_censo_demografico.setor_censitario_raca_idade_genero_2010`
        WHERE SUBSTR(id_setor_censitario, 1, 7) IN ({muni_list})
    """
    df = bd.read_sql(sql, billing_project_id=billing)
    return df.set_index('id_setor_censitario').fillna(0)


def _age_2022(munis: tuple[int], billing: str) -> pd.DataFrame:
    """Busca e calcula faixas etárias para o Censo 2022."""
    muni_sql = ", ".join(f"'{int(m):07d}'" for m in munis)
    blk_20_64 = _sql_sum(range(645, 654), width=5, prefix="IFNULL(V", suffix=", 0)")
    blk_65p = _sql_sum((654, 655, 656),  width=5, prefix="IFNULL(V", suffix=", 0)")
    sql = f"""
        SELECT
          id_setor_censitario,
          pessoas AS total_pop, V00644 AS age_15_19,
          ({blk_20_64}) AS age_20_64, ({blk_65p}) AS age_65p
        FROM `{_BD_TABLE_SETOR_2022}`
        WHERE SUBSTR(id_setor_censitario, 1, 7) IN ({muni_sql})
    """
    df = bd.read_sql(sql, billing_project_id=billing).fillna(0)
    df["age_0_14"] = df["total_pop"] - df[["age_15_19", "age_20_64", "age_65p"]].sum(axis=1)
    return df.drop(columns="total_pop").set_index("id_setor_censitario")


def _race_2022(muni_ids: tuple[int], billing: str) -> pd.DataFrame:
    """Busca e imputa totais de raça/cor para o Censo 2022."""
    muni_sql = ", ".join(f"'{int(m):07d}'" for m in muni_ids)
    pop_15p_sql = _sql_sum(range(644, 657), prefix="IFNULL(V", suffix=",0)")
    race_block = lambda off: _sql_sum((b + off for b in range(657, 717, 5)), prefix="IFNULL(V", suffix=",0)")
    races_15p_sql = ",\n".join(f"({race_block(i)}) AS race_{r}_15p" for i, r in enumerate(_CENSO_RACES))
    sql = f"""
        SELECT id_setor_censitario, pessoas AS total_pop,
               ({pop_15p_sql}) AS pop_15p, {races_15p_sql}
        FROM `{_BD_TABLE_SETOR_2022}`
        WHERE SUBSTR(id_setor_censitario, 1, 7) IN ({muni_sql})
    """
    df = bd.read_sql(sql, billing_project_id=billing).fillna(0)
    df["age_0_14"] = df["total_pop"] - df["pop_15p"]
    df["id_mun"] = df["id_setor_censitario"].str.slice(0, 7)
    race_cols = [f"race_{r}_15p" for r in _CENSO_RACES]
    grouped = df.groupby("id_mun")[race_cols + ["pop_15p"]].sum()
    muni_share = grouped[race_cols].div(grouped["pop_15p"], axis=0)
    for r in _CENSO_RACES:
        df[f"race_{r}"] = (
            df[f"race_{r}_15p"] +
            df["age_0_14"] * df["id_mun"].map(muni_share[f"race_{r}_15p"])
        )
    return (df.set_index(df.id_setor_censitario.str.zfill(15))
              [[f"race_{r}" for r in _CENSO_RACES]])

# =============================================================================
# FUNÇÕES AUXILIARES DE BAIXO NÍVEL
# =============================================================================

def _sql_sum(
    codes: Iterable[int],
    *,
    width: int = 5,
    prefix: str = "V",
    suffix: str = "",
) -> str:
    """Constrói uma soma formatada para o BigQuery."""
    return " + ".join(f"{prefix}{c:0{width}d}{suffix}" for c in codes)