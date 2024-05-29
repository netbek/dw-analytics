{% macro dbt_audit_columns(relation_alias=none) -%}
    {% for column_name in var('dbt_audit_column_names') -%}
        {% if relation_alias %}{{ relation_alias }}.{% endif %}{{ adapter.quote(column_name) }} as {{ column_name }}{% if not loop.last %},{% endif %}
    {% endfor %}
{%- endmacro %}
