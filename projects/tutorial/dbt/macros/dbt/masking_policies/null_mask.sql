{% macro null_mask(expression, data_type) -%}
    {{ adapter.dispatch('null_mask', 'default')(expression, data_type) }}
{%- endmacro %}
