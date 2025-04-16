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
    doc->'raw_data'->'prices'->>'tfr' as prices_tfr,
    doc->'raw_data'->'prices'->>'vatRate' as vatRate,
    doc->'raw_data'->'prices'->>'ucdPrice' as ucdPrice,
    doc->'raw_data'->'prices'->>'sellingPrice' as sellingPrice,
    doc->'raw_data'->'prices'->>'repaymentRate' as repaymentRate,
    doc->'raw_data'->'prices'->>'reimbursementBase' as reimbursementBase,
    (doc->>'extracted_date')::timestamp as extracted_date,
    (doc->>'deleted')::boolean as is_deleted
from {{ source('pharma_sources', 'mycollection') }} 