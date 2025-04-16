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
    doc->'properties'->>'productName' as product_name,
    doc->'properties'->>'productDosage' as product_dosage,
    doc->'properties'->>'galenicFormCode' as galenic_form_code,
    doc->'properties'->>'galenicFormLabel' as galenic_form_label,
    doc->'properties'->>'aspect' as aspect,
    doc->'properties'->>'thumbnail' as thumbnail,
    doc->'properties'->>'subLaboratories' as subLaboratories,
    doc->'properties'->>'inFacilityFormulary' as inFacilityFormulary,
    doc->'properties'->>'galenicFormLabelHtml' as galenicFormLabelHtml,
    doc->'properties'->>'segmentationAttributes' as segmentationAttributes,
    doc->'properties'->'intakeUnits' as intake_units
from {{ source('pharma_sources', 'mycollection') }} 