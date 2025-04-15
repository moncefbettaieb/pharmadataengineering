{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='categorie_id')}}

SELECT categorie_id, taxonomy_id, similarity_score, categorie, sous_categorie_1, sous_categorie_2, sous_categorie_3, combined_categorie, taxonomy_category, taxonomy_sub_category1, taxonomy_sub_category2, taxonomy_sub_category3, combined_taxonomy
FROM (
    SELECT categorie_id, taxonomy_id, similarity_score, categorie, sous_categorie_1, sous_categorie_2, sous_categorie_3, combined_categorie, taxonomy_category, taxonomy_sub_category1, taxonomy_sub_category2, taxonomy_sub_category3, combined_taxonomy,
           ROW_NUMBER() OVER (PARTITION BY categorie_id ORDER BY similarity_score DESC) AS rn
    FROM {{ ref('int_similarity_scores_categorie_taxonomy') }}
) sub
WHERE rn = 1