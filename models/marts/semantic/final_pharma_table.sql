{{ config(materialized='table') }}

WITH enriched AS (
    SELECT *
    FROM {{ ref('stg_enriched_pharmacie_unification') }}
),

deduplicated AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cip_code ORDER BY last_update DESC) AS rn
        FROM enriched
    ) sub
    WHERE rn = 1
)

SELECT
    *,
    CURRENT_TIMESTAMP AS last_update
FROM deduplicated
WHERE cip_code IS NOT NULL
  AND cip_code <> ''
  AND cip_code <> 'null'
  AND brand IS NOT NULL
  AND brand <> ''
  AND brand <> 'null'
  AND title IS NOT NULL
  AND title <> ''
  AND title <> 'null'
  AND categorie IS NOT NULL
  AND categorie <> ''
  AND categorie <> 'null'
  AND combined_category IS NOT NULL
  AND combined_category <> ''
  AND combined_category <> 'null'
  AND short_desc IS NOT NULL
  AND short_desc <> ''
  AND short_desc <> 'null'
  AND long_desc IS NOT NULL
  AND long_desc <> ''
  AND long_desc <> 'null'
  AND age_minimum IS NOT NULL
  AND age_minimum <> ''
  AND age_minimum <> 'null'
  AND nombre_d_unites IS NOT NULL
  AND nombre_d_unites <> ''
  AND nombre_d_unites <> 'null'
  AND indication_contre_indication IS NOT NULL
  AND indication_contre_indication <> ''
  AND indication_contre_indication <> 'null'
  AND last_update IS NOT NULL
  AND last_update <> ''
  AND last_update <> 'null'