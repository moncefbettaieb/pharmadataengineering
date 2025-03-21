{% snapshot snapshot_pharma_gdd %}
    {{
      config(
        target_schema='uat',
        unique_key='cip_code',
        strategy='timestamp',
        updated_at='update_at'
      )
    }}

    SELECT
        cip_code,
        title,
        brand,
        CAST(_ab_cdc_updated_at AS TIMESTAMP) AS update_at
    FROM {{ source('pharma_sources', 'raw_pharma_gdd') }}

{% endsnapshot %}
