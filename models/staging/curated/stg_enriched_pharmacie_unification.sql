{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='cip_code',
    post_hook=[
        create_index(this, 'cip_code')
    ]
) }}

    SELECT 
        unif.*,
        dim_ct.taxonomy_category, 
        dim_ct.taxonomy_sub_category1, 
        dim_ct.taxonomy_sub_category2, 
        dim_ct.taxonomy_sub_category3
    FROM {{ ref('stg_pharmacie_unification') }} unif
    LEFT JOIN {{ ref('dim_match_categorie_taxonomy') }} dim_ct
    ON  unif.categorie = dim_ct.categorie
    AND unif.sous_categorie_1 = dim_ct.sous_categorie_1
    AND unif.sous_categorie_2 = dim_ct.sous_categorie_2
    AND unif.sous_categorie_3 = dim_ct.sous_categorie_3
    AND unif.combined_categorie = dim_ct.combined_categorie