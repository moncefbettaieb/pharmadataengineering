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
    doc->'labelHtml' as labelHtml,
    doc->'labels'->>'label' as label,
    doc->'labels'->>'longLabel' as long_label,
    doc->'labels'->>'typeLabel' as type_label,
    doc->'labels'->>'shortLabel' as short_label,
    doc->'labels'->>'searchLabel' as search_label,
    doc->'labels'->>'longLabelHtml' as long_label_html,
    doc->'labels'->>'shortLabelHtml' as short_label_html
from {{ source('pharma_sources', 'mycollection') }} 