{# Adapted from https://github.com/dbt-labs/dbt-labs-experimental-features/blob/42f36a4418dd4f7f6b0bd36c03dcc3ec01bb3304/insert_by_period/macros/get_period_sql.sql #}
{% macro clickhouse__get_period_sql(sql, column, period, min_value, max_value, offset) -%}
  {%- set batch_load_predicates -%}
    (
      "{{ column }}" >= toDateTime64('{{ min_value }}', 6) + interval '{{ offset }} {{ period }}' and
      "{{ column }}" <  toDateTime64('{{ min_value }}', 6) + interval '{{ offset + 1 }} {{ period }}' and
      "{{ column }}" <= toDateTime64('{{ max_value }}', 6)
    )
  {%- endset -%}
  {%- set filtered_sql = sql | replace('__BATCH_LOAD_PREDICATES__', batch_load_predicates) -%}
  {{ return(filtered_sql) }}
{%- endmacro %}
