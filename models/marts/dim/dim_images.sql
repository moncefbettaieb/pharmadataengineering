{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='cip_code || source || image_url',
    post_hook=[
        create_index(this, 'cip_code')
    ]
) }}

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
    false AS downloaded,
    CURRENT_TIMESTAMP AS last_update
FROM enumerated

{% if is_incremental() %}
-- Éviter les doublons déjà téléchargés
WHERE (cip_code, source, image_url) NOT IN (
    SELECT cip_code, source, image_url
    FROM {{ this }}
    WHERE downloaded = true
)
{% endif %}

ORDER BY image_id