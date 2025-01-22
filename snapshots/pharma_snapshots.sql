{% snapshot pharma_gdd_snapshot %}
{{ config(
    unique_key='cip_code',
    strategy='timestamp',
    updated_at='last_update_timestamp'
) }}

SELECT
    brand,
    title,
    cip_code,
    categorie,
    image_src,
    CURRENT_TIMESTAMP AS last_update_timestamp
FROM {{ ref('stg_pharma_gdd') }}

{% endsnapshot %}
