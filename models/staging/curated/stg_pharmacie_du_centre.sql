{{ config(materialized='table') }}

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
        last_update, 
        "Age_minimum" as age_minimum,
        product_price, 
        "Conditionnement" as conditionnement, 
        first_insertion, 
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
    title,  
    source,  
    cip_code, 
    categorie,
    image_src, 
    long_desc, 
    short_desc, 
    age_minimum,
    product_price, 
    conditionnement, 
    first_insertion, 
    substance_active, 
    sous_categorie_1, 
    sous_categorie_2, 
    sous_categorie_3,
    categorie || ' ' || sous_categorie_1 || ' ' || sous_categorie_2 || ' ' || sous_categorie_3 as combined_category,
    nature_de_produit, 
    nombre_d_unites, 
    indication_contre_indication,
    CAST((last_update->>'update') AS TIMESTAMP) AS last_update
FROM raw_data
WHERE cip_code IS NOT NULL
AND title IS NOT NULL
AND brand IS NOT NULL
AND categorie IS NOT NULL