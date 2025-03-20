{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

WITH raw_data AS (
    SELECT
        cip_code,
        title,
        brand,
        source,
        categorie,
        sous_categorie_1,
        sous_categorie_2,
        short_desc,
        long_desc,
        posologie,
        composition,
        conditionnement,
        contre_indication,
        image_links,
        processed_time
    FROM {{ source('pharma_sources', 'raw_pharma_gdd') }}
)

SELECT
    cip_code,
    title,
    brand,
    source,
    categorie,
    sous_categorie_1,
    sous_categorie_2,
    short_desc,
    long_desc,
    posologie::TEXT AS posologie,
    composition::TEXT AS composition,
    conditionnement,
    contre_indication::TEXT AS contre_indication,
    image_links,
    processed_time,
    categorie || ' ' || sous_categorie_1 || ' ' || sous_categorie_2 AS combined_category,
    CURRENT_TIMESTAMP AS last_update
FROM raw_data
WHERE cip_code IS NOT NULL
  AND title IS NOT NULL
  AND brand IS NOT NULL
  AND categorie IS NOT NULL