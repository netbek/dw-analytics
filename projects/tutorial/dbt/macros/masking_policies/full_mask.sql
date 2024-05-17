{% macro full_mask(expression, data_type) -%}
    case
        when false then {{ expression }}
        else cast({{ dbt_privacy.mask(expression) }}, {% if 'Nullable' in data_type %}'{{ data_type }}'{% else %}'Nullable({{ data_type }})'{% endif %})
    end
{%- endmacro %}
