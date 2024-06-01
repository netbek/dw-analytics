{% macro dbt_audit(cte_ref) %}
    select
        *,

        toDateTime64({{ dbt.current_timestamp() }}, 6) as _dbt_run_time,

        {% if is_incremental() %}
            'incremental'
        {% else %}
            'full'
        {% endif %} as _dbt_run_type,

        {% if is_incremental() %}
            assumeNotNull((select toInt64(max(_dbt_run_num)) from {{ this }}) + 1)
        {% else %}
            toInt64(1)
        {% endif %} as _dbt_run_num,

        {% if is_incremental() %}
            assumeNotNull((select toDateTime64(min(_dbt_last_full_refresh_time), 6) from {{ this }}))
        {% else %}
            toDateTime64('{{ run_started_at.replace(tzinfo=None).isoformat() }}', 6)
        {% endif %} as _dbt_last_full_refresh_time
    from {{ cte_ref }}
{% endmacro %}
