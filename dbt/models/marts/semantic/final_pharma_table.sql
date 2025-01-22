{{ config(materialized='table') }}

WITH enriched AS (
    SELECT *
    FROM {{ ref('stg_enriched_pharmacie_unification') }}
)

SELECT
    *
FROM enriched e
