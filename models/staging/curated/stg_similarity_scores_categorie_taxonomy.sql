
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='categorie_id, taxonomy_id') }}

WITH combined_pharmacie AS (
    SELECT
        categorie_id AS categorie_id,
        categorie,
        sous_categorie_1,
        sous_categorie_2,
        sous_categorie_3,
        combined_categorie
    FROM {{ ref('dim_pharma_categorie') }}
    WHERE categorie IS NOT NULL
        AND categorie <> ''
        AND categorie <> 'null'
        AND combined_categorie IS NOT NULL
        AND combined_categorie <> ''
        AND combined_categorie <> 'null'
),
combined_taxonomy AS (
    SELECT
        taxonomy_id,
        taxonomy_name,
        taxonomy_category,
        taxonomy_sub_category1,
        taxonomy_sub_category2,
        taxonomy_sub_category3,
        combined_taxonomy
    FROM {{ ref('dim_product_taxonomy') }}
)
    SELECT
        p.categorie_id,
        p.combined_categorie,
        p.categorie,
        p.sous_categorie_1,
        p.sous_categorie_2,
        p.sous_categorie_3,
        t.taxonomy_id,
        t.taxonomy_name,
        t.taxonomy_category,
        t.taxonomy_sub_category1,
        t.taxonomy_sub_category2,
        t.taxonomy_sub_category3,
        t.combined_taxonomy,
        SIMILARITY(p.combined_categorie, t.combined_taxonomy) AS similarity_score,
        CURRENT_TIMESTAMP AS last_update
    FROM combined_pharmacie p
    CROSS JOIN combined_taxonomy t