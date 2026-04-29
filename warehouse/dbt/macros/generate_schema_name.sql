-- generate_schema_name.sql
-- Override dbt default schema naming to use custom_schema_name directly,
-- instead of prefixing with target schema.
-- This ensures models land in 'gold', 'staging', etc. (not 'dev_gold').

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
