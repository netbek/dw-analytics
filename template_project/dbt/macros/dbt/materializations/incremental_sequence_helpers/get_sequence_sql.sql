{% macro clickhouse__get_sequence_sql(sql, column, batch_size, range_min, range_max, offset, model_alias=none) -%}
  {%- set col %}{% if model_alias %}{{ model_alias }}.{% endif %}{{ adapter.quote(column) }}{% endset -%}
  {%- set batch_predicates -%}
    (
      {{ col }} >= {{ range_min }} + ({{ batch_size }} * {{ offset }}) and
      {{ col }} <  {{ range_min }} + ({{ batch_size }} * {{ offset + 1 }}) and
      {{ col }} <= {{ range_max }}
    )
  {%- endset -%}
  {%- set filtered_sql = sql | replace('__BATCH_PREDICATES__', batch_predicates) -%}
  {{ return(filtered_sql) }}
{%- endmacro %}
