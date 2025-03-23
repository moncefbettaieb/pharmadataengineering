{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='categorie_id',
    post_hook=[
        create_gin_index(this, 'combined_categorie')
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
    CONCAT_WS(' ', categorie, sous_categorie_1, sous_categorie_2, sous_categorie_3) as combined_categorie,
    CURRENT_TIMESTAMP AS last_update
FROM raw_categories
WHERE categorie IS NOT NULL
  AND categorie <> ''
  AND categorie <> 'null'