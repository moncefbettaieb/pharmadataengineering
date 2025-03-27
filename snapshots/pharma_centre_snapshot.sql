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
        cip_code::TEXT as cip_code,
        title::TEXT as title,
        brand::TEXT as brand,
        categorie::TEXT as categorie,
        sous_categorie_1::TEXT as sous_categorie_1,
        sous_categorie_2::TEXT as sous_categorie_2,
        sous_categorie_3::TEXT as sous_categorie_3, 
        long_desc,
        short_desc,
        "Label"::TEXT as label,
        "Volume"::TEXT as volume,
        "Age_minimum"::TEXT as age_minimum,
        "Conditionnement"::TEXT as conditionnement,
        "Specificite_s_"::TEXT as specificites,
        "Substance_active"::TEXT as substance_active,
        "Nature_de_produit"::TEXT as nature_de_produit,
        "Nombre_d_unites"::TEXT as nombre_d_unites,
        "Indication___Contre_indication"::TEXT as indication_contre_indication,
        product_price::TEXT as product_price, 
        processed_time, 
        source,         
        image_src,
        url_source,
        CAST(_ab_cdc_updated_at AS TIMESTAMP) as updated_at
    FROM {{ source('pharma_sources', 'raw_pharmacie_du_centre') }}

{% endsnapshot %}
