{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['doc_id']},
      {'columns': ['code']},
      {'columns': ['code13']}
    ]
  )
}}

with source as (
    select * from {{ ref('stg_api_service_data') }}
),

-- Rank rows by extracted_date to get the most recent version for each code13
ranked as (
    select
        *,
        row_number() over (
            partition by code13
            order by extracted_date desc, id desc
        ) as rn
    from source
    where code13 is not null  -- Exclude records without code13
),

-- Keep only the most recent version
deduplicated as (
    select
        id,
        doc_id,
        code,
        type,
        code13,
        label,
        long_label,
        type_label,
        short_label,
        search_label,
        long_label_html,
        short_label_html,
        cip7,
        cip13,
        cip13_referent,
        product_name,
        product_dosage,
        galenic_form_code,
        galenic_form_label,
        is_homeo,
        is_doping,
        is_generic,
        is_internal,
        is_narcotic,
        is_referent,
        is_crushable,
        is_biosimilar,
        is_pediatrics,
        drive_warning,
        is_ghs,
        is_vaccin,
        needs_fridge,
        needs_freezer,
        intake_units,
        extracted_date,
        is_deleted
    from ranked
    where rn = 1
)

select * from deduplicated 