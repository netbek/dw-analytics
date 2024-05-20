{# Adapted from https://github.com/dbt-labs/dbt-labs-experimental-features/blob/42f36a4418dd4f7f6b0bd36c03dcc3ec01bb3304/insert_by_period/macros/insert_by_period_materialization.sql #}
{% macro clickhouse__batch_period_create_table(temporary, relation, sql) %}
  {%- set batch_period = config.get('batch_period', 'week') -%}
  {%- set batch_column = config.require('batch_column') -%}
  {%- set batch_source_relation = config.get('batch_source_relation') -%}
  {%- set batch_source_relation_alias = config.get('batch_source_relation_alias') -%}

  {%- if batch_source_relation -%}
    {%- set min_max = select_min_max(batch_source_relation, batch_column) | as_native -%}
    {%- set range_min = min_max['min'][0].strftime('%Y-%m-%d') -%}
    {%- set range_max = min_max['max'][0].strftime('%Y-%m-%d') -%}
  {%- else -%}
    {%- set batch_column = config.require('range_column') -%}
    {%- set range_min = config.require('range_min') -%}
    {%- set range_max = config.get('range_max') -%}
  {%- endif -%}

  {%- if sql.find('__BATCH_PREDICATES__') == -1 -%}
    {%- set error_message -%}
      Model '{{ model.unique_id }}' does not include the required string '__BATCH_PREDICATES__' in its sql
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}

  {%- set boundaries = clickhouse__get_period_boundaries(batch_period, range_min, range_max) | as_native -%}
  {%- set range_min = boundaries['range_min'][0] -%}
  {%- set range_max = boundaries['range_max'][0] -%}
  {%- set num_batches = boundaries['num_batches'][0] | int -%}

  -- commit each batch as a separate transaction
  {% for offset in range(num_batches) -%}
    {%- set msg = "Loading batch " ~ (offset + 1) ~ " of " ~ (num_batches) -%}
    {{ print(msg) }}

    {%- set filtered_sql = clickhouse__get_period_sql(sql, batch_column, batch_period, range_min, range_max, offset, model_alias=batch_source_relation_alias) -%}

    {% call statement('main') %}
      {% if offset == 0 %}
        {{ get_create_table_as_sql(temporary, relation, filtered_sql) }}
      {% else %}
        {{ clickhouse__insert_into(relation, filtered_sql) }}
      {% endif %}
    {% endcall %}
  {% endfor %}
{% endmacro %}
