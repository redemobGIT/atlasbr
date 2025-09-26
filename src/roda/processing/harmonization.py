import geopandas as gpd

def schools_to_rais(schools):
    return(schools.loc[schools.rede == 'Publica'].assign(quantidade_vinculos_ativos=lambda x: (
        x['quantidade_docente_educacao_basica'] + ['quantidade_profissional']
    ),
    grupo_cnae = 'P',
    ).reindex(
        columns=[
            'quantidade_vinculos_ativos', 'grupo_cnae', 'id_municipio', 'geometry'
            ]
        )
    )


def aggregate_cnes_jobs_by_cep(
    cnes_gdf: gpd.GeoDataFrame,
    *, 
    cep_col: str = "cep",
    workers_col: str = "quantidade_trabalhadores_saude",
    outputs_jobs_col: str = "quantidade_vinculos_ativos",
    cnae_group: str = "Q"
) -> gpd.geodataframe:
    dissolved = (
        cnes_gdf.loc[(cnes_gdf.complexidade.notnull()) # Hospital or Ambulatory Care
        * (cnes_gdf.indicator_vinculo_sus == '1') # SUS-Linked
        ].dissolve(
            by=['id_municipio', cep_col],
            aggfunc={
                workers_col: "sum"
            }
        ).rename(columns={
            workers_col: outputs_jobs_col
        }
        ).assign(grupo_cnae=cnae_group).reset_index()
    )

    return gpd.geodataframe(dissolved, geometry="geometry", crs=cnes_gdf.crs)