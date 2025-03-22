{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='cip_code'
) }}

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
    *
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