{% macro postgres__full_mask(expression, data_type) -%}
    case
        when false then {{ expression }}
        else {{ dbt_privacy.mask(expression) }}
    end
{%- endmacro %}
