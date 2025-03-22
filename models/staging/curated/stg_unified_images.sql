{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='cip_code || source || image_url'
) }}

WITH gdd AS (
    SELECT
        cip_code,
        source,
        image_links
    FROM {{ ref('snapshot_pharma_gdd') }}
    WHERE image_links IS NOT NULL
),

centre AS (
    SELECT
        cip_code,
        source,
        image_src  AS image_url
    FROM {{ ref('snapshot_pharma_centre') }}
    WHERE image_src IS NOT NULL
    AND image_src LIKE '%' || source || '%'
),

pharma_gdd_images AS (
    SELECT
        gdd.cip_code,
        source,
        jsonb_array_elements_text(gdd.image_links) AS image_url
    FROM gdd
    WHERE gdd.image_links IS NOT NULL
)

SELECT DISTINCT
    cip_code,
    source,
    image_url
FROM (
    SELECT * FROM pharma_gdd_images
    UNION ALL
    SELECT * FROM centre
) t
WHERE image_url LIKE '%' || source || '%'