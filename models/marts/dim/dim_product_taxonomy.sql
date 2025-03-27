{{ config(materialized='table',
    post_hook=[
        create_gin_index(this, 'combined_taxonomy')
    ]) }}
    
WITH base AS (
    SELECT
        CAST(taxonomy_id AS BIGINT) AS taxonomy_id, 
        taxonomy_name
    FROM {{ ref('cleaned_google_product_taxonomy') }} 
),

split_categories AS (
    SELECT
        taxonomy_id,
        taxonomy_name,
        SPLIT_PART(taxonomy_name, '>', 1) AS taxonomy_category,
        NULLIF(SPLIT_PART(taxonomy_name, '>', 2), '') AS taxonomy_sub_category1,
        NULLIF(SPLIT_PART(taxonomy_name, '>', 3), '') AS taxonomy_sub_category2,
        NULLIF(SPLIT_PART(taxonomy_name, '>', 4), '') AS taxonomy_sub_category3
    FROM base
)

SELECT
    taxonomy_id,
    taxonomy_name,
    TRIM(taxonomy_category) AS taxonomy_category,
    TRIM(taxonomy_sub_category1) AS taxonomy_sub_category1,
    TRIM(taxonomy_sub_category2) AS taxonomy_sub_category2,
    TRIM(taxonomy_sub_category3) AS taxonomy_sub_category3,
    CONCAT_WS(' ', TRIM(taxonomy_category), TRIM(taxonomy_sub_category1), TRIM(taxonomy_sub_category2), TRIM(taxonomy_sub_category3)) AS combined_taxonomy
FROM split_categories
ORDER BY taxonomy_id