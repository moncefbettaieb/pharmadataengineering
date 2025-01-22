{% macro coalesce_nullable(column, default_value) %}
    COALESCE({{ column }}, {{ default_value }})
{% endmacro %}
