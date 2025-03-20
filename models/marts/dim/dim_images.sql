{{ config(
    materialized='incremental',
    incremental_strategy='append',
    post_hook=[
        create_index(this, 'cip_code')
    ]) }}

WITH unified AS (
    SELECT
        cip_code,
        source,
        image_url,
        last_update
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
    false AS downloaded,
    CURRENT_TIMESTAMP AS last_update
FROM enumerated
ORDER BY image_id
