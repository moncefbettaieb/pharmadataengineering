{{ config(
    materialized='incremental',
    incremental_strategy='append',
    post_hook=[
        create_index(this, 'cip_code')
    ]
) }}

WITH stg_gdd_filtered AS (
    SELECT
        gdd.cip_code,
        gdd.brand,
        gdd.title,
        gdd.source,
        gdd.categorie,
        gdd.sous_categorie_1,
        gdd.sous_categorie_2,
        NULL AS sous_categorie_3,
        gdd.combined_category,
        gdd.short_desc,
        gdd.long_desc,
        NULL AS age_minimum,
        NULL AS nombre_d_unites,
        NULL AS indication_contre_indication,
        gdd.posologie,
        gdd.composition,
        gdd.contre_indication
    FROM {{ ref('stg_pharma_gdd') }} AS gdd
    {{ filter_last_update('gdd', 'last_update') }}
),

stg_centre_filtered AS (
    SELECT
        centre.cip_code,
        centre.brand,
        centre.title,
        centre.source,
        centre.categorie,
        centre.sous_categorie_1, 
        centre.sous_categorie_2, 
        centre.sous_categorie_3,
        centre.combined_category,
        centre.short_desc,
        centre.long_desc,
        centre.age_minimum,
        centre.nombre_d_unites,
        centre.indication_contre_indication,
        NULL AS posologie,
        NULL AS composition,
        NULL AS contre_indication
    FROM {{ ref('stg_pharmacie_du_centre') }} AS centre
    {{ filter_last_update('centre', 'last_update') }}
),

{# 2) On réunit les données filtrées des deux sources #}
unified AS (
    SELECT *
    FROM stg_gdd_filtered

    UNION ALL

    SELECT *
    FROM stg_centre_filtered
),

{# 3) On déduplique par cip_code si nécessaire. #}
deduplicated AS (
    SELECT
        cip_code,
        COALESCE((ARRAY_AGG(brand))[1], (ARRAY_AGG(brand))[2]) AS brand,
        COALESCE((ARRAY_AGG(title))[1], (ARRAY_AGG(title))[2]) AS title,
        COALESCE((ARRAY_AGG(source))[1], (ARRAY_AGG(source))[2]) AS source,
        COALESCE((ARRAY_AGG(categorie))[1], (ARRAY_AGG(categorie))[2]) AS categorie,
        COALESCE((ARRAY_AGG(sous_categorie_1))[1], (ARRAY_AGG(sous_categorie_1))[2]) AS sous_categorie_1,
        COALESCE((ARRAY_AGG(sous_categorie_2))[1], (ARRAY_AGG(sous_categorie_2))[2]) AS sous_categorie_2,
        COALESCE((ARRAY_AGG(sous_categorie_3))[1], (ARRAY_AGG(sous_categorie_3))[2]) AS sous_categorie_3,
        COALESCE((ARRAY_AGG(combined_category))[1], (ARRAY_AGG(combined_category))[2]) AS combined_category,
        COALESCE((ARRAY_AGG(short_desc))[1], (ARRAY_AGG(short_desc))[2]) AS short_desc,
        COALESCE((ARRAY_AGG(long_desc))[1], (ARRAY_AGG(long_desc))[2]) AS long_desc,
        COALESCE((ARRAY_AGG(age_minimum))[1], (ARRAY_AGG(age_minimum))[2]) AS age_minimum,
        COALESCE((ARRAY_AGG(nombre_d_unites))[1], (ARRAY_AGG(nombre_d_unites))[2]) AS nombre_d_unites,
        COALESCE((ARRAY_AGG(indication_contre_indication))[1], (ARRAY_AGG(indication_contre_indication))[2]) AS indication_contre_indication,
        COALESCE((ARRAY_AGG(posologie))[1], (ARRAY_AGG(posologie))[2]) AS posologie,
        COALESCE((ARRAY_AGG(composition))[1], (ARRAY_AGG(composition))[2]) AS composition,
        COALESCE((ARRAY_AGG(contre_indication))[1], (ARRAY_AGG(contre_indication))[2]) AS contre_indication,
        CURRENT_TIMESTAMP AS last_update
    FROM unified
    GROUP BY cip_code
)

SELECT *
FROM deduplicated
WHERE brand IS NOT NULL
  AND brand <> ''
  AND title IS NOT NULL
  AND title <> ''
  AND categorie IS NOT NULL
  AND categorie <> ''
