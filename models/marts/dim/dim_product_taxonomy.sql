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
        SPLIT_PART(taxonomy_name, '>', 1) AS category,
        NULLIF(SPLIT_PART(taxonomy_name, '>', 2), '') AS sub_category1,
        NULLIF(SPLIT_PART(taxonomy_name, '>', 3), '') AS sub_category2,
        NULLIF(SPLIT_PART(taxonomy_name, '>', 4), '') AS sub_category3
    FROM base
)

SELECT
    taxonomy_id,
    taxonomy_name,
    TRIM(category) AS category,
    TRIM(sub_category1) AS sub_category1,
    TRIM(sub_category2) AS sub_category2,
    TRIM(sub_category3) AS sub_category3,
    TRIM(category) || COALESCE(' ' || TRIM(sub_category1), '') || COALESCE(' ' || TRIM(sub_category2), '') || COALESCE(' ' || TRIM(sub_category3), '') as combined_taxonomy
FROM split_categories
ORDER BY taxonomy_id