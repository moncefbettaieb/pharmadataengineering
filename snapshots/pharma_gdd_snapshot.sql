{% snapshot snapshot_pharma_gdd %}
    {{
      config(
        target_schema='uat',
        unique_key='cip_code',
        strategy='timestamp',
        updated_at='updated_at'
      )
    }}

    SELECT
        cip_code,
        title,
        brand,
        source,
        categorie,
        sous_categorie_1,
        sous_categorie_2,
        short_desc,
        long_desc,
        posologie,
        composition,
        conditionnement,
        contre_indication,
        image_links,
        processed_time,
        CAST(_ab_cdc_updated_at AS TIMESTAMP) AS update_at
    FROM {{ source('pharma_sources', 'raw_pharma_gdd') }}

{% endsnapshot %}
