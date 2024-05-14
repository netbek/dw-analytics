{% macro clickhouse__get_sequence_sql(sql, column, batch_size, range_min, range_max, offset) -%}
  {%- set batch_predicates -%}
    (
      "{{ column }}" >= {{ range_min }} + ({{ batch_size }} * {{ offset }}) and
      "{{ column }}" <  {{ range_min }} + ({{ batch_size }} * {{ offset + 1 }}) and
      "{{ column }}" <= {{ range_max }}
    )
  {%- endset -%}
  {%- set filtered_sql = sql | replace('__BATCH_PREDICATES__', batch_predicates) -%}
  {{ return(filtered_sql) }}
{%- endmacro %}
