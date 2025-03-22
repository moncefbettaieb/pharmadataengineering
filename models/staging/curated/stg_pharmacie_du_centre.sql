{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='cip_code'
) }}

WITH raw_data AS (
    SELECT
        *
    FROM {{ ref('snapshot_pharma_centre') }}
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
    brand, 
    title::TEXT AS title,  
    source,  
    cip_code, 
    categorie::TEXT AS categorie,
    image_src, 
    long_desc, 
    short_desc, 
    age_minimum,
    product_price, 
    conditionnement, 
    processed_time, 
    substance_active, 
    sous_categorie_1::TEXT AS sous_categorie_1, 
    sous_categorie_2::TEXT AS sous_categorie_2, 
    sous_categorie_3::TEXT AS sous_categorie_3,
    CONCAT(categorie::TEXT, ' ', sous_categorie_1::TEXT, ' ', sous_categorie_2::TEXT, ' ', sous_categorie_3::TEXT) AS combined_category,
    nature_de_produit, 
    nombre_d_unites, 
    indication_contre_indication,
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
  AND title::TEXT <> ''
  AND title::TEXT <> 'null'
  AND categorie IS NOT NULL
  AND categorie::TEXT <> ''
  AND categorie::TEXT <> 'null'
