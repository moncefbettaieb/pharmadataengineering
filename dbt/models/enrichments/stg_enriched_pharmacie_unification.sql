{{ config(materialized='table') }}

WITH unified AS (
    SELECT 
        unif.*,
        dim_ct.categorie_id
    FROM {{ ref('stg_pharmacie_unification') }} unif
    LEFT JOIN {{ ref('dim_pharma_categorie') }} dim_ct
    ON unif.combined_category = dim_ct.combined_category
),
unified_with_taxo AS (
SELECT
    u.*,
    match.taxonomy_id
FROM unified u
LEFT JOIN {{ ref('inter_match_categorie_taxonomy') }} match 
ON u.categorie_id = match.categorie_id
)

SELECT
    utx.*,
    dim_tx.taxonomy_name,
    dim_tx.category,
    dim_tx.sub_category1,
    dim_tx.sub_category2,
    dim_tx.sub_category3
FROM unified_with_taxo utx
LEFT JOIN {{ ref('dim_product_taxonomy') }} dim_tx
ON utx.taxonomy_id = dim_tx.taxonomy_id