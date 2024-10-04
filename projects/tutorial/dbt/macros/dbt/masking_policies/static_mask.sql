{% macro static_mask(expression, data_type) -%}
    {{ adapter.dispatch('static_mask', 'default')(expression, data_type) }}
{%- endmacro %}
