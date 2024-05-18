{% macro dbt_audit_columns() %}
    {{ var('dbt_audit_column_names') | join(', ') }}
{% endmacro %}
