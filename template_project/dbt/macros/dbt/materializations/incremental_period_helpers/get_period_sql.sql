{# Adapted from https://github.com/dbt-labs/dbt-labs-experimental-features/blob/42f36a4418dd4f7f6b0bd36c03dcc3ec01bb3304/insert_by_period/macros/get_period_sql.sql #}
{% macro clickhouse__get_period_sql(sql, column, period, range_min, range_max, offset, model_alias=none) -%}
  {%- set col %}{% if model_alias %}{{ model_alias }}.{% endif %}{{ adapter.quote(column) }}{% endset -%}
  {%- set batch_predicates -%}
    (
      {{ col }} >= toDateTime64('{{ range_min }}', 6) + interval '{{ offset }} {{ period }}' and
      {{ col }} <  toDateTime64('{{ range_min }}', 6) + interval '{{ offset + 1 }} {{ period }}' and
      {{ col }} <= toDateTime64('{{ range_max }}', 6)
    )
  {%- endset -%}
  {%- set filtered_sql = sql | replace('__BATCH_PREDICATES__', batch_predicates) -%}
  {{ return(filtered_sql) }}
{%- endmacro %}
