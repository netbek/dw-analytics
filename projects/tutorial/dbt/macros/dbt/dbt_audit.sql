{% macro dbt_audit(cte_ref) %}
    select
        *,
        cityHash64('{{ invocation_id }}', '{{ model.unique_id }}') as dbt_run_id
    from {{ cte_ref }}
{% endmacro %}
