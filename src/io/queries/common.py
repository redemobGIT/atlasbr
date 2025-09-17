# Column presets for common datasets

RAIS_VINC_COLS = [
    "ano",
    "id_estabelecimento",
    "id_municipio",
    "sigla_uf",
    "cnae_2_subclasse",
    "idade",
    "raca_cor",
    "sexo",
    "vl_remun_media_sm",
]

RAIS_ESTAB_COLS = [
    "ano",
    "id_estabelecimento",
    "id_municipio",
    "sigla_uf",
    "cnae_2_subclasse",
    "natureza_juridica",
    "tam_estab",
]

CENSO_SETOR_COLS = [
    "ano",
    "id_setor_censitario",
    "cod_municipio",
    "sigla_uf",
    "v002",  # população total, etc.
]

CENSO_POP_COLS = [
    "ano",
    "id_setor_censitario",
    "cod_municipio",
    "sigla_uf",
    "v0601",  # exemplo: população por sexo
]

INEP_ESCOLAS_COLS = [
    "ano",
    "CO_ENTIDADE",
    "CO_MUNICIPIO",
    "SG_UF",
    "NO_ENTIDADE",
]

INEP_TURMAS_COLS = [
    "ano",
    "CO_TURMA",
    "CO_ENTIDADE",
    "TP_ETAPA_ENSINO",
]

CNES_ESTAB_COLS = [
    "ano",
    "CO_CNES",
    "CO_MUNICIPIO_GESTOR",
    "SG_UF",
    "NO_RAZAO_SOCIAL",
]

CNES_PROF_COLS = [
    "ano",
    "CO_PROFISSIONAL_SUS",
    "CO_CNES",
    "CO_MUNICIPIO",
    "SG_UF",
    "CO_CBO",
]
