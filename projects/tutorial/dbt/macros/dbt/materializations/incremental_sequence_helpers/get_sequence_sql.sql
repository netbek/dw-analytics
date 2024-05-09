{% macro clickhouse__get_sequence_sql(sql, column, batch_size, min_value, max_value, offset) -%}
  {%- set batch_load_predicates -%}
    (
      "{{ column }}" >= {{ min_value }} + ({{ batch_size }} * {{ offset }}) and
      "{{ column }}" <  {{ min_value }} + ({{ batch_size }} * {{ offset + 1 }}) and
      "{{ column }}" <= {{ max_value }}
    )
  {%- endset -%}
  {%- set filtered_sql = sql | replace('__BATCH_LOAD_PREDICATES__', batch_load_predicates) -%}
  {{ return(filtered_sql) }}
{%- endmacro %}
