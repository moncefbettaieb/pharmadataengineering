{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='cip_code',
    post_hook=[
        create_index(this, 'cip_code')
    ]
) }}

SELECT
    *
FROM {{ ref('stg_enriched_pharmacie_unification') }}
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