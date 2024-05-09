{% macro select_min_max(model, column) -%}
  {% set sql %}
    select
      min({{ column }}) as min,
      max({{ column }}) as max
    from {{ model }}
  {% endset %}
  {{ return(dbt_utils.get_query_results_as_dict(sql)) }}
{%- endmacro %}
