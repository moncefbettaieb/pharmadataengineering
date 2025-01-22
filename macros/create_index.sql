{% macro create_index(table, column) %}
CREATE INDEX IF NOT EXISTS idx_{{ column }} ON {{ table }} ({{ column }});
{% endmacro %}

{% macro create_gin_index(table, column) %}
CREATE INDEX IF NOT EXISTS idx_{{ column }} ON {{ table }} USING gin ({{ column }} gin_trgm_ops);
{% endmacro %}
