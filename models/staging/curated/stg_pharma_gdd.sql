{{  config(materialized='table') }}

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
        last_update, 
        first_insertion
        
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
        posologie, 
        composition, 
        conditionnement, 
        contre_indication,
        image_links, 
        last_update, 
        first_insertion,
        categorie || ' ' || sous_categorie_1 || ' ' || sous_categorie_2 as combined_category
FROM raw_data
WHERE cip_code IS NOT NULL
AND title IS NOT NULL
AND brand IS NOT NULL
AND categorie IS NOT NULL