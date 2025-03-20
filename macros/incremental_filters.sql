{% macro filter_last_update(source_alias, column_name='last_update') %}
    {% if is_incremental() %}
        CROSS JOIN (
            SELECT COALESCE(MAX({{ column_name }}), '1900-01-01'::timestamp) AS max_col
            FROM {{ this }}
        ) AS max_t
        WHERE {{ source_alias }}.{{ column_name }} > max_t.max_col
    {% endif %}
{% endmacro %}