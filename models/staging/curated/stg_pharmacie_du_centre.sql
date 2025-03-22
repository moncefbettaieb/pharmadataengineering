{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

WITH raw_data AS (
    SELECT
        *
    FROM {{ ref('pharma_centre_snapshot') }}
    WHERE dbt_valid_to IS NULL
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
