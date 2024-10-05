{% macro static_mask(expression, data_type) -%}
    {{ adapter.dispatch('static_mask')(expression, data_type) }}
{%- endmacro %}


{% macro clickhouse__static_mask(expression, data_type) -%}
    case
        when false then {{ expression }}
        when {{ expression }} is null then null
        else repeat('*', 8)
    end
{%- endmacro %}


{% macro postgres__static_mask(expression, data_type) -%}
    case
        when false then {{ expression }}
        when {{ expression }} is null then null
        else repeat('*', 8)
    end
{%- endmacro %}
