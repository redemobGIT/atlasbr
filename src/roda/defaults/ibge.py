IBGE_AREAS_URBANIZADAS_BASE_URL = (
    "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
    "tipologias_do_territorio/areas_urbanizadas_do_brasil"
)

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

CFG = {
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

BD_TABLE_SETOR_2022 = "basedosdados.br_ibge_censo_2022.setor_censitario"

CENSO_RACES = ("branca", "preta", "amarela", "parda", "indigena")

INCOME_2022_URL = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Agregados_por_Setores_Censitarios_Rendimento_do_Responsavel/"
    "Agregados_por_setores_renda_responsavel_BR_csv.zip"
)