{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='cip_code',
    post_hook=[
        create_index(this, 'cip_code')
    ]
) }}

WITH unified AS (
    SELECT
        gdd.cip_code,
        gdd.brand,
        gdd.title,
        gdd.source,
        gdd.categorie,
        gdd.sous_categorie_1,
        gdd.sous_categorie_2,
        NULL AS sous_categorie_3,
        gdd.combined_categorie,
        gdd.short_desc,
        gdd.long_desc,
        NULL as age_minimum,
        NULL as nombre_d_unites,
        NULL as indication_contre_indication,
        gdd.usage,
        gdd.posologie,
        gdd.composition,
        gdd.presentation,
        gdd.composition_fp,
        gdd.conditionnement,
        gdd.contre_indication,
        NULL AS label,
        NULL AS volume,
        NULL AS specificites,
        NULL AS substance_active,
        NULL AS nature_de_produit,
        COALESCE(last_update, CURRENT_TIMESTAMP) AS last_update
    FROM {{ ref('stg_pharma_gdd') }} gdd

    UNION ALL

    SELECT
        centre.cip_code,
        centre.brand,
        centre.title,
        centre.source,
        centre.categorie,
        centre.sous_categorie_1, 
        centre.sous_categorie_2, 
        centre.sous_categorie_3, 
        centre.combined_categorie,
        centre.short_desc,
        centre.long_desc,
        centre.age_minimum,
        centre.nombre_d_unites,
        centre.indication_contre_indication,
        NULL AS usage,
        NULL AS posologie,
        NULL AS composition,
        NULL AS presentation,
        NULL AS composition_fp,
        NULL AS conditionnement,
        NULL AS contre_indication,
        centre.label,
        centre.volume,
        centre.specificites,
        centre.substance_active,
        centre.nature_de_produit,
        COALESCE(last_update, CURRENT_TIMESTAMP) AS last_update
    FROM {{ ref('stg_pharmacie_du_centre') }} centre
),

deduplicated AS (
    SELECT
        cip_code,
        COALESCE((ARRAY_AGG(brand))[0], COALESCE((ARRAY_AGG(brand))[1], (ARRAY_AGG(brand))[2])) AS brand,
        COALESCE((ARRAY_AGG(title))[0], COALESCE((ARRAY_AGG(title))[1], (ARRAY_AGG(title))[2])) AS title,
        COALESCE((ARRAY_AGG(source))[0], COALESCE((ARRAY_AGG(source))[1], (ARRAY_AGG(source))[2])) AS source,
        COALESCE((ARRAY_AGG(categorie))[0], COALESCE((ARRAY_AGG(categorie))[1], (ARRAY_AGG(categorie))[2])) AS categorie,
        COALESCE((ARRAY_AGG(sous_categorie_1))[0], COALESCE((ARRAY_AGG(sous_categorie_1))[1], (ARRAY_AGG(sous_categorie_1))[2])) AS sous_categorie_1,
        COALESCE((ARRAY_AGG(sous_categorie_2))[0], COALESCE((ARRAY_AGG(sous_categorie_2))[1], (ARRAY_AGG(sous_categorie_2))[2])) AS sous_categorie_2,
        COALESCE((ARRAY_AGG(sous_categorie_3))[0], COALESCE((ARRAY_AGG(sous_categorie_3))[1], (ARRAY_AGG(sous_categorie_3))[2])) AS sous_categorie_3,
        COALESCE((ARRAY_AGG(combined_categorie))[0], COALESCE((ARRAY_AGG(combined_categorie))[1], (ARRAY_AGG(combined_categorie))[2])) AS combined_categorie,
        COALESCE((ARRAY_AGG(short_desc))[0], COALESCE((ARRAY_AGG(short_desc))[1], (ARRAY_AGG(short_desc))[2])) AS short_desc,
        COALESCE((ARRAY_AGG(long_desc))[0], COALESCE((ARRAY_AGG(long_desc))[1], (ARRAY_AGG(long_desc))[2])) AS long_desc,
        COALESCE((ARRAY_AGG(age_minimum))[0], COALESCE((ARRAY_AGG(age_minimum))[1], (ARRAY_AGG(age_minimum))[2])) AS age_minimum,
        COALESCE((ARRAY_AGG(nombre_d_unites))[0], COALESCE((ARRAY_AGG(nombre_d_unites))[1], (ARRAY_AGG(nombre_d_unites))[2])) AS nombre_d_unites,
        COALESCE((ARRAY_AGG(indication_contre_indication))[0], COALESCE((ARRAY_AGG(indication_contre_indication))[1], (ARRAY_AGG(indication_contre_indication))[2])) AS indication_contre_indication,
        COALESCE((ARRAY_AGG(usage))[0], COALESCE((ARRAY_AGG(usage))[1], (ARRAY_AGG(usage))[2])) AS usage,
        COALESCE((ARRAY_AGG(posologie))[0], COALESCE((ARRAY_AGG(posologie))[1], (ARRAY_AGG(posologie))[2])) AS posologie,
        COALESCE((ARRAY_AGG(composition))[0], COALESCE((ARRAY_AGG(composition))[1], (ARRAY_AGG(composition))[2])) AS composition,
        COALESCE((ARRAY_AGG(presentation))[0], COALESCE((ARRAY_AGG(presentation))[1], (ARRAY_AGG(presentation))[2])) AS presentation,
        COALESCE((ARRAY_AGG(composition_fp))[0], COALESCE((ARRAY_AGG(composition_fp))[1], (ARRAY_AGG(composition_fp))[2])) AS composition_fp,
        COALESCE((ARRAY_AGG(conditionnement))[0], COALESCE((ARRAY_AGG(conditionnement))[1], (ARRAY_AGG(conditionnement))[2])) AS conditionnement,
        COALESCE((ARRAY_AGG(contre_indication))[0], COALESCE((ARRAY_AGG(contre_indication))[1], (ARRAY_AGG(contre_indication))[2])) AS contre_indication,
        COALESCE((ARRAY_AGG(label))[0], COALESCE((ARRAY_AGG(label))[1], (ARRAY_AGG(label))[2])) AS label,
        COALESCE((ARRAY_AGG(volume))[0], COALESCE((ARRAY_AGG(volume))[1], (ARRAY_AGG(volume))[2])) AS volume,
        COALESCE((ARRAY_AGG(specificites))[0], COALESCE((ARRAY_AGG(specificites))[1], (ARRAY_AGG(specificites))[2])) AS specificites,
        COALESCE((ARRAY_AGG(substance_active))[0], COALESCE((ARRAY_AGG(substance_active))[1], (ARRAY_AGG(substance_active))[2])) AS substance_active,
        COALESCE((ARRAY_AGG(nature_de_produit))[0], COALESCE((ARRAY_AGG(nature_de_produit))[1], (ARRAY_AGG(nature_de_produit))[2])) AS nature_de_produit,
        COALESCE((ARRAY_AGG(last_update))[0], COALESCE((ARRAY_AGG(last_update))[1], CURRENT_TIMESTAMP)) AS last_update
    FROM unified
    GROUP BY cip_code
)

SELECT *
FROM deduplicated
WHERE cip_code IS NOT NULL
  AND cip_code <> ''
  AND cip_code <> 'null'
  AND brand IS NOT NULL
  AND brand <> ''
  AND brand <> 'null'
  AND title IS NOT NULL
  AND title <> ''
  AND title <> 'null'
  AND categorie IS NOT NULL
  AND categorie <> ''
  AND categorie <> 'null'