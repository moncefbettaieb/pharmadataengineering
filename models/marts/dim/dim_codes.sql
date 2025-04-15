{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='code_id')}}

WITH codes AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY cip_code) AS code_id,
        cip_code
    FROM {{ ref('int_pharmacie_unification') }}
)

SELECT
    code_id,
    cip_code as code,
    CURRENT_TIMESTAMP AS last_update
FROM codes