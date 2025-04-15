{% snapshot snapshot_api_service %}
    {{
      config(
        target_schema='uat',
        unique_key='code13',
        strategy='timestamp',
        updated_at='extracted_date'
      )
    }}

    SELECT
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
    FROM {{ ref('stg_api_service_data') }}

{% endsnapshot %} 