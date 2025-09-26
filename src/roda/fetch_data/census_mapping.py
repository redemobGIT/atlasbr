"""
Este módulo centraliza o mapeamento de dados para a coleta de diferentes
temas do Censo Demográfico do IBGE, a partir da plataforma Base dos Dados.

Ele define quais tabelas, colunas e/ou funções externas devem ser usadas
para cada tema e ano.
"""

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
            external="_income_2022",   # handled by dedicated fetcher
        ),
    },
    "age": {
        # explicit external fetcher (see §2)
        2010: {"external": "_age_2010"},
        2022: {"external": "_age_2022"},   # stub: returns NaNs
    },

    # ── NEW: self-declared race totals ────────────────────────────────────
    "race": {
        2010: {"external": "_race_2010"},
        2022: {"external": "_race_2022"},  # stub
    },
}


# --- Constantes e Esquemas Adicionais do Censo ---

# Tabela de setores censitários de 2022 na plataforma Base dos Dados
BD_TABLE_SETOR_2022 = "basedosdados.br_ibge_censo_2022.setor_censitario"

# Categorias de raça/cor autodeclarada utilizadas pelo IBGE
CENSO_RACES = ("branca", "preta", "amarela", "parda", "indigena")