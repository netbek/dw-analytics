{% macro min_max_select(model, column) -%}
  {% set sql %}
    select
      min({{ column }}) as min,
      max({{ column }}) as max
    from {{ model }}
  {% endset %}
  {{ return(dbt_utils.get_query_results_as_dict(sql)) }}
{%- endmacro %}
