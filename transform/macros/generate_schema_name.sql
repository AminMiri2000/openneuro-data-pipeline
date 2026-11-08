{#
    Use the configured schema name as-is ("staging", "marts", ...).

    dbt's default prefixes it with the target schema ("main_staging"), which is
    useful when many developers share one warehouse. Here each developer has
    their own DuckDB file, so the shorter names are clearer.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
