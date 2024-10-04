{% macro full_mask(expression, data_type) -%}
    {{ adapter.dispatch('full_mask', 'default')(expression, data_type) }}
{%- endmacro %}
