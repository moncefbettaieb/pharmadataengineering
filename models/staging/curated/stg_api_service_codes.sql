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
    doc->'properties'->'codes'->>'cip7' as cip7,
    doc->'properties'->'codes'->>'cip13' as cip13,
    doc->'properties'->'codes'->>'cip13Referent' as cip13_referent,
    doc->'properties'->'codes'->>'cis' as cis,
    doc->'properties'->'codes'->>'ucd7' as ucd7,
    doc->'properties'->'codes'->>'ean13' as ean13,
    doc->'properties'->'codes'->>'ucd13' as ucd13,
    doc->'properties'->'codes'->>'code13Referent' as code13Referent
from {{ source('pharma_sources', 'mycollection') }} 