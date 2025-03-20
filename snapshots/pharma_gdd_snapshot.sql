{% snapshot pharma_gdd_snapshot %}
    {{
      config(
        target_schema='my_schema_snapshots',
        unique_key='cip_code',
        strategy='timestamp',
        updated_at='processed_time'
      )
    }}

    SELECT
        cip_code,
        title,
        brand,
        processed_time
    FROM {{ source('pharma_sources', 'raw_pharma_gdd') }}

{% endsnapshot %}
