{% snapshot snapshot_pharma_centre %}
    {{
      config(
        target_schema="uat",
        unique_key='cip_code',
        strategy='timestamp',
        updated_at='updated_at'
      )
    }}

    SELECT
        brand, 
        title::TEXT as title,  
        source,  
        cip_code, 
        categorie::TEXT as categorie,
        image_src, 
        long_desc, 
        short_desc, 
        "Age_minimum" as age_minimum,
        product_price, 
        "Conditionnement" as conditionnement, 
        processed_time, 
        "Substance_active" as substance_active, 
        sous_categorie_1::TEXT as sous_categorie_1, 
        sous_categorie_2::TEXT as sous_categorie_2, 
        sous_categorie_3::TEXT as sous_categorie_3, 
        "Nature_de_produit" as nature_de_produit, 
        "Nombre_d_unites" as nombre_d_unites, 
        "Indication___Contre_indication" as indication_contre_indication,
        CAST(_ab_cdc_updated_at AS TIMESTAMP) as updated_at
    FROM {{ source('pharma_sources', 'raw_pharmacie_du_centre') }}

{% endsnapshot %}
