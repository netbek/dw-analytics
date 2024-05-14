{% macro clickhouse__get_sequence_boundaries(batch_size, range_min, range_max) -%}
  {% set sql %}
    select
      {{ range_min }} as range_min,
      {{ range_max }} as range_max,
      greatest(1, ceil(({{ range_max }} - {{ range_min }})::real / {{ batch_size }})) as num_batches
  {% endset %}
  {{ return(dbt_utils.get_query_results_as_dict(sql)) }}
{%- endmacro %}
