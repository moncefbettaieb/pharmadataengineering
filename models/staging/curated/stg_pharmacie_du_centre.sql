{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

WITH raw_data AS (
    SELECT
        brand, 
        title,  
        source,  
        cip_code, 
        categorie,
        image_src, 
        long_desc, 
        short_desc, 
        "Age_minimum" as age_minimum,
        product_price, 
        "Conditionnement" as conditionnement, 
        processed_time, 
        "Substance_active" as substance_active, 
        sous_categorie_1, 
        sous_categorie_2, 
        sous_categorie_3, 
        "Nature_de_produit" as nature_de_produit, 
        "Nombre_d_unites" as nombre_d_unites, 
        "Indication___Contre_indication" as indication_contre_indication
    FROM {{ source('pharma_sources', 'raw_pharmacie_du_centre') }}
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
    CONCAT(categorie, ' ', sous_categorie_1, ' ', sous_categorie_2, ' ', sous_categorie_3) AS combined_category,
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
