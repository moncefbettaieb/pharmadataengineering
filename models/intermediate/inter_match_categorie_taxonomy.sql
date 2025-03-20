{{ config(materialized='table') }}

WITH max_similarity AS (
    SELECT
        p.categorie_id,
        b.taxonomy_id,
        MAX(b.similarity_score) AS max_similarity_score
    FROM {{ ref('dim_pharma_categorie') }} p
    LEFT JOIN  {{ ref('inter_similarity_scores_categorie_taxonomy') }} b ON p.categorie_id = b.taxonomy_id
    GROUP BY p.categorie_id, b.taxonomy_id
)

SELECT
    p.*,
    m.taxonomy_id,
    m.max_similarity_score AS similarity_score
FROM {{ ref('dim_pharma_categorie') }} p
LEFT JOIN max_similarity m ON p.categorie_id = m.categorie_id