{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

WITH raw_data AS (
    SELECT
        *
    FROM {{ ref('snap_pharma_gdd') }}
    WHERE dbt_valid_to IS NULL
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
    CURRENT_TIMESTAMP AS last_update
FROM raw_data
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