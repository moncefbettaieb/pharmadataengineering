{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='cip_code'
) }}

WITH raw_data AS (
    SELECT
        *
    FROM {{ ref('snapshot_pharma_gdd') }}
)

SELECT
    TRIM(BOTH '"' FROM cip_code::TEXT) AS cip_code,
    TRIM(BOTH '"' FROM title::TEXT) AS title,
    TRIM(BOTH '"' FROM brand::TEXT) AS brand,
    TRIM(BOTH '"' FROM categorie::TEXT) AS categorie,
    TRIM(BOTH '"' FROM sous_categorie_1::TEXT )AS sous_categorie_1,
    TRIM(BOTH '"' FROM sous_categorie_2::TEXT) AS sous_categorie_2,
    CONCAT_WS(' ', TRIM(BOTH '"' FROM categorie), TRIM(BOTH '"' FROM sous_categorie_1), TRIM(BOTH '"' FROM sous_categorie_2)) as combined_categorie,
    TRIM(BOTH '"' FROM long_desc) AS long_desc,
    TRIM(BOTH '"' FROM short_desc) AS short_desc,
    TRIM(BOTH '"' FROM usage::TEXT) AS usage,
    TRIM(BOTH '"' FROM posologie::TEXT) AS posologie, 
    TRIM(BOTH '"' FROM composition::TEXT) AS composition, 
    TRIM(BOTH '"' FROM presentation::TEXT) AS presentation, 
    TRIM(BOTH '"' FROM composition_fp::TEXT) AS composition_fp, 
    TRIM(BOTH '"' FROM conditionnement::TEXT) AS conditionnement, 
    TRIM(BOTH '"' FROM contre_indication::TEXT) AS contre_indication,
    image_links,
    source,
    url_source,
    processed_time,
    COALESCE(updated_at, CURRENT_TIMESTAMP) AS last_update
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