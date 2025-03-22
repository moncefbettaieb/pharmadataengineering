{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='cip_code'
) }}

WITH raw_data AS (
    SELECT
        *
    FROM {{ ref('snapshot_pharma_gdd') }}
),

last_versions AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cip_code
            ORDER BY updated_at DESC
        ) AS rn
    FROM raw_data
)


SELECT
    cip_code,
    title,
    brand,
    source,
    categorie::TEXT AS categorie,
    sous_categorie_1::TEXT AS sous_categorie_1,
    sous_categorie_2::TEXT AS sous_categorie_2,
    short_desc,
    long_desc,
    posologie::TEXT AS posologie,
    composition::TEXT AS composition,
    conditionnement,
    contre_indication::TEXT AS contre_indication,
    image_links,
    processed_time,
    categorie::TEXT || ' ' || sous_categorie_1::TEXT || ' ' || sous_categorie_2::TEXT AS combined_category,
    COALESCE(updated_at, CURRENT_TIMESTAMP) AS last_update
FROM last_versions
WHERE rn = 1
  AND cip_code IS NOT NULL
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