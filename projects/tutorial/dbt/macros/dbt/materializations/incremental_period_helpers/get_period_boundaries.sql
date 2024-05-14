{# Adapted from https://github.com/dbt-labs/dbt-labs-experimental-features/blob/42f36a4418dd4f7f6b0bd36c03dcc3ec01bb3304/insert_by_period/macros/get_period_boundaries.sql #}
{% macro clickhouse__get_period_boundaries(period, range_min, range_max) -%}
  {% set sql %}
    with data as (
      select
        toDateTime64('{{ range_min|trim }}', 6) as range_min,
        {% if range_max -%}
          toDateTime64('{{ range_max|trim }}', 6) + interval '1 day' - interval '1 microsecond'
        {% else %}
          toDateTime64({{ dbt.current_timestamp() }}, 6)
        {%- endif %} as range_max
    )

    select
      range_min,
      range_max,
      {{ datediff('range_min', 'range_max', period) }} + 1 as num_batches
    from data
  {% endset %}
  {{ return(dbt_utils.get_query_results_as_dict(sql)) }}
{%- endmacro %}
