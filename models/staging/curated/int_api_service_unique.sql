{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['doc_id']}
    ]
  )
}}

with ranked as (
    select
        *,
        row_number() over (
            partition by code13
            order by extracted_date desc, id desc
        ) as rn
    from {{ ref('int_api_service_merged') }}
    where code13 is not null
)

select
    *
from ranked
where rn = 1 