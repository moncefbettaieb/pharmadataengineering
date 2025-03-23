{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='cip_code'
) }}

WITH raw_data AS (
    SELECT
        *
    FROM {{ ref('snapshot_pharma_centre') }}
)

SELECT
    TRIM(BOTH '"' FROM cip_code) AS cip_code,
    TRIM(BOTH '"' FROM title) AS title,
    TRIM(BOTH '"' FROM brand) AS brand,
    TRIM(BOTH '"' FROM categorie) AS categorie,
    TRIM(BOTH '"' FROM sous_categorie_1) AS sous_categorie_1,
    TRIM(BOTH '"' FROM sous_categorie_2) AS sous_categorie_2,
    TRIM(BOTH '"' FROM sous_categorie_3) AS sous_categorie_3,
    CONCAT_WS(' ', TRIM(BOTH '"' FROM categorie::TEXT), TRIM(BOTH '"' FROM sous_categorie_1::TEXT), TRIM(BOTH '"' FROM sous_categorie_2::TEXT), TRIM(BOTH '"' FROM sous_categorie_3::TEXT)) AS combined_categorie,
    TRIM(BOTH '"' FROM long_desc) AS long_desc,
    TRIM(BOTH '"' FROM short_desc) AS short_desc,
    TRIM(BOTH '"' FROM label) AS label,
    TRIM(BOTH '"' FROM volume) AS volume,
    TRIM(BOTH '"' FROM age_minimum) AS age_minimum,
    TRIM(BOTH '"' FROM conditionnement) AS conditionnement,
    TRIM(BOTH '"' FROM specificites) AS specificites,
    TRIM(BOTH '"' FROM substance_active) AS substance_active,
    TRIM(BOTH '"' FROM nature_de_produit) AS nature_de_produit,
    TRIM(BOTH '"' FROM nombre_d_unites) AS nombre_d_unites,
    TRIM(BOTH '"' FROM indication_contre_indication) AS indication_contre_indication,
    product_price, 
    processed_time, 
    source,         
    image_src,
    url_source,
    COALESCE(updated_at, CURRENT_TIMESTAMP) AS last_update
FROM raw_data
WHERE cip_code IS NOT NULL
  AND cip_code <> ''
  AND cip_code <> 'null'
  AND brand IS NOT NULL
  AND brand <> ''
  AND brand <> 'null'
  AND title IS NOT NULL
  AND title::TEXT <> ''
  AND title::TEXT <> 'null'
  AND categorie IS NOT NULL
  AND categorie::TEXT <> ''
  AND categorie::TEXT <> 'null'