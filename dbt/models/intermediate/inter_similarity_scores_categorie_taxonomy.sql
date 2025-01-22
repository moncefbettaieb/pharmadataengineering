
{{ config(materialized='table') }}

WITH combined_pharmacie AS (
    SELECT
        categorie_id AS categorie_id,
        categorie,
        sous_categorie_1,
        sous_categorie_2,
        sous_categorie_3,
        combined_category
    FROM {{ ref('dim_pharma_categorie') }}
    WHERE categorie IS NOT NULL
    AND combined_category IS NOT NULL
),
combined_taxonomy AS (
    SELECT
        taxonomy_id,
        taxonomy_name,
        category,
        sub_category1,
        sub_category2,
        sub_category3,
        combined_taxonomy
    FROM {{ ref('dim_product_taxonomy') }}
)
    SELECT
        p.categorie_id,
        p.combined_category,
        p.categorie,
        p.sous_categorie_1,
        p.sous_categorie_2,
        p.sous_categorie_3,
        t.taxonomy_id,
        t.taxonomy_name,
        t.category,
        t.sub_category1,
        t.sub_category2,
        t.sub_category3,
        t.combined_taxonomy,
        SIMILARITY(p.combined_category, t.combined_taxonomy) AS similarity_score
    FROM combined_pharmacie p
    CROSS JOIN combined_taxonomy t