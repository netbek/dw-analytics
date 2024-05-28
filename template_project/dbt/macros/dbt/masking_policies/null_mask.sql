{% macro null_mask(expression, data_type) -%}
    case
        when false then {{ expression }}
        else cast(null, {% if 'Nullable' in data_type %}'{{ data_type }}'{% else %}'Nullable({{ data_type }})'{% endif %})
    end
{%- endmacro %}
