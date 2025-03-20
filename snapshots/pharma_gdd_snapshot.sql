{% snapshot pharma_gdd_snapshot %}
    {{
      config(
        target_schema='pharma_schema_snapshots',
        unique_key='cip_code',
        strategy='timestamp',
        updated_at='processed_time'
      )
    }}

    SELECT
        cip_code,
        title,
        brand,
        CAST(processed_time AS TIMESTAMP) AS processed_time
    FROM {{ source('pharma_sources', 'raw_pharma_gdd') }}

{% endsnapshot %}
