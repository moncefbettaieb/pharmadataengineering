{{ config(materialized='table',
    post_hook=[
        create_gin_index(this, 'combined_category')
    ]) }}

WITH raw_categories AS (
    SELECT DISTINCT
        categorie,
        sous_categorie_1,
        sous_categorie_2,
        NULL as sous_categorie_3
    FROM {{ ref('stg_pharma_gdd') }}
    UNION
    SELECT DISTINCT
        categorie,
        sous_categorie_1,
        sous_categorie_2,
        sous_categorie_3
    FROM {{ ref('stg_pharmacie_du_centre') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY categorie) AS categorie_id,
    categorie,
    sous_categorie_1,
    sous_categorie_2,
    sous_categorie_3,
    categorie || ' ' || sous_categorie_1 || ' ' || sous_categorie_2 || ' ' || sous_categorie_3 as combined_category
FROM raw_categories
WHERE categorie IS NOT NULL
  AND categorie <> ''