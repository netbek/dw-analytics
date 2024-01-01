{% macro dbt_audit_columns() %}
    _dbt_build_time,
    _dbt_build_type,
    _dbt_build_num,
    _dbt_last_full_refresh
{% endmacro %}
