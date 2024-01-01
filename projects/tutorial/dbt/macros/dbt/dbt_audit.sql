{% macro dbt_audit(cte_ref) %}
    select
        *,

        toDateTime({{ dbt.current_timestamp() }}) as _dbt_build_time,

        {% if is_incremental() %}
            'incremental'
        {% else %}
            'full'
        {% endif %} as _dbt_build_type,

        {% if is_incremental() %}
            assumeNotNull((select toInt64(max(_dbt_build_num)) from {{ this }}) + 1)
        {% else %}
            toInt64(1)
        {% endif %} as _dbt_build_num,

        {% if is_incremental() %}
            assumeNotNull((select toDateTime(min(_dbt_last_full_refresh)) from {{ this }}))
        {% else %}
            toDateTime('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}')
        {% endif %} as _dbt_last_full_refresh
    from {{ cte_ref }}
{% endmacro %}
