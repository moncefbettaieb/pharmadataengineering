{{ config(materialized='table',
    post_hook=[
        create_index(this, 'cip_code')
    ]) }}

WITH unified AS (
    SELECT
        cip_code,
        source,
        image_url
    FROM {{ ref('stg_unified_images') }}
)

, enumerated AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY cip_code, image_url) AS image_id,
        cip_code,
        source,
        image_url
    FROM unified
)

SELECT
    image_id,
    cip_code,
    source,
    image_url,
    NULL AS gcs_path,
    false AS downloaded
FROM enumerated
ORDER BY image_id
