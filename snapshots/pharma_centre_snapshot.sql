{% snapshot pharma_centre_snapshot %}
    {{
      config(
        target_schema="{{ target.schema }}",
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
    FROM {{ source('pharma_sources', 'raw_pharmacie_du_centre') }}

{% endsnapshot %}
