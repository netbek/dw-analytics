{% macro clickhouse__get_sequence_boundaries(batch_size, min_value, max_value) -%}
  {% set sql %}
    select
      {{ min_value }} as min_value,
      {{ max_value }} as max_value,
      greatest(1, ceil(({{ max_value }} - {{ min_value }})::real / {{ batch_size }})) as num_batches
  {% endset %}
  {{ return(dbt_utils.get_query_results_as_dict(sql)) }}
{%- endmacro %}
