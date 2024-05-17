{% macro apply_masking_policy(expression, column_name) -%}
    {%- if model['columns'] -%}
        {%- set data_type = model['columns'][column_name]['data_type'] -%}
        {%- set meta = model['columns'][column_name].get('meta', {}) -%}
        {%- set masking_policy = meta.get('masking_policy') -%}

        {%- if masking_policy -%}
            {{ context.get(masking_policy)(expression, data_type) }} as {{ column_name }}
        {%- else -%}
            {{ expression }} as {{ column_name }}
        {%- endif -%}
    {%- else -%}
        {{ expression }} as {{ column_name }}
    {%- endif -%}
{%- endmacro %}
