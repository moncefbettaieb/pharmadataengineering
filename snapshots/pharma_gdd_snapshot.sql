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
        cip_code::TEXT AS cip_code,
        title::TEXT AS title,
        brand::TEXT AS brand,
        categorie::TEXT AS categorie, 
        sous_categorie_1::TEXT AS sous_categorie_1, 
        sous_categorie_2::TEXT AS sous_categorie_2, 
        long_desc, 
        short_desc,
        usage::TEXT AS usage, 
        posologie::TEXT AS posologie, 
        composition::TEXT AS composition, 
        presentation::TEXT AS presentation, 
        composition_fp::TEXT AS composition_fp, 
        conditionnement::TEXT AS conditionnement, 
        contre_indication::TEXT AS contre_indication,
        image_links,
        source,
        url_source,
        processed_time,
        CAST(_ab_cdc_updated_at AS TIMESTAMP) AS updated_at
    FROM {{ source('pharma_sources', 'raw_pharma_gdd') }}

{% endsnapshot %}
