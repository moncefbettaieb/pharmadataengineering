{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['doc_id']}
    ]
  )
}}

select
    id,
    doc->>'_id' as doc_id,
    doc->>'code' as code,
    (doc->>'type')::integer as type,
    doc->>'code13' as code13,
    doc->>'productId' as productId,
    (doc->>'extracted_date')::timestamp as extracted_date,
    (doc->>'deleted')::boolean as is_deleted
from {{ source('pharma_sources', 'mycollection') }} 