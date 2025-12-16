WITH estab AS (
    SELECT
        e.id_estabelecimento_cnes,
        e.id_municipio,
        e.cep,
        e.tipo_unidade,
        e.tipo_pessoa,
        e.indicador_vinculo_sus,
        e.indicador_atencao_hospitalar,

        CASE
          WHEN REGEXP_CONTAINS(
                  TO_JSON_STRING(e),
                  r'"indicador_gestao_alta_[^"]+":\\s*1')
          THEN 'alta'
          WHEN REGEXP_CONTAINS(
                  TO_JSON_STRING(e),
                  r'"indicador_gestao_media_[^"]+":\\s*1')
          THEN 'media'
          WHEN REGEXP_CONTAINS(
                  TO_JSON_STRING(e),
                  r'"indicador_gestao_basica_[^"]+":\\s*1')
          THEN 'basica'
        END AS complexidade,

        {{infra_sql}}

    FROM `basedosdados.br_ms_cnes.estabelecimento` AS e
    WHERE e.id_municipio IN ({{muni_sql}})
      AND e.ano = {{ano}}
      AND e.mes = {{mes}}
      AND e.tipo_unidade IN ({{unit_sql}})
      AND e.tipo_pessoa = '3'
),

workers AS (
    SELECT
        t.id_estabelecimento_cnes,
        COALESCE(
          SUM(
            CASE
              WHEN SAFE_CAST(num AS INT64) = 88888 THEN 0
              ELSE SAFE_CAST(num AS INT64)
            END
          ),
          0
        ) AS quantidade_trabalhadores_saude
    FROM `basedosdados.br_ms_cnes.profissional` AS t
    LEFT JOIN UNNEST(
      REGEXP_EXTRACT_ALL(
        TO_JSON_STRING(t),
        r'"quantidade_profissional_[^"]+":\\s*([0-9]+)'
      )
    ) AS num ON TRUE
    WHERE t.id_municipio IN ({{muni_sql}})
      AND t.ano = {{ano}}
      AND t.mes = {{mes}}
    GROUP BY t.id_estabelecimento_cnes
)

SELECT
    e.*,
    COALESCE(w.quantidade_trabalhadores_saude, 0)
      AS quantidade_trabalhadores_saude
FROM estab   AS e
LEFT JOIN workers AS w USING (id_estabelecimento_cnes)
