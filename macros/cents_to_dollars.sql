{# A basic example for a project-wide macro to cast a column uniformly #}

{% macro centsToToDollars(columnName) -%}
    {{ return(adapter.dispatch('centsToToDollars')(columnName)) }}
{%- endmacro %}

{% macro default__centsToToDollars(columnName) -%}
    ({{ columnName }} / 100)::numeric(16, 2)
{%- endmacro %}

{% macro postgres__centsToToDollars(columnName) -%}
    ({{ columnName }}::numeric(16, 2) / 100)
{%- endmacro %}

{% macro bigquery__centsToToDollars(columnName) %}
    round(cast(({{ columnName }} / 100) as numeric), 2)
{% endmacro %}

{% macro fabric__centsToToDollars(columnName) %}
    cast({{ columnName }} / 100 as numeric(16,2))
{% endmacro %}