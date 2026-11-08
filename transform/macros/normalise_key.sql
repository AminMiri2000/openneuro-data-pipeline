{#
    Normalise free text for matching against a lookup seed:
    lower case, trimmed, runs of spaces/hyphens/underscores collapsed to "_".
    "Right-Handed", " right handed " and "RIGHT_HANDED" all become "right_handed".
#}
{% macro normalise_key(expression) -%}
    regexp_replace(lower(trim({{ expression }})), '[\s_-]+', '_', 'g')
{%- endmacro %}
