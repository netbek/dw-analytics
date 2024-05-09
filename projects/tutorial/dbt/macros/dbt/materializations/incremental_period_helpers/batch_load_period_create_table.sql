{# Adapted from https://github.com/dbt-labs/dbt-labs-experimental-features/blob/42f36a4418dd4f7f6b0bd36c03dcc3ec01bb3304/insert_by_period/macros/insert_by_period_materialization.sql #}
{% macro clickhouse__batch_load_period_create_table(temporary, relation, sql) %}
  {%- set batch_load_period = config.get('batch_load_period', 'week') -%}
  {%- set batch_load_column = config.require('batch_load_column') -%}
  {%- set batch_load_source_model = config.get('batch_load_source_model') -%}

  {%- if batch_load_source_model -%}
    {%- set min_max = select_min_max(batch_load_source_model, batch_load_column) | as_native -%}
    {%- set batch_load_min_value = min_max['min'][0].strftime('%Y-%m-%d') -%}
    {%- set batch_load_max_value = min_max['max'][0].strftime('%Y-%m-%d') -%}
  {%- else -%}
    {%- set batch_load_min_value = config.require('batch_load_min_value') -%}
    {%- set batch_load_max_value = config.get('batch_load_max_value') -%}
  {%- endif -%}

  {%- if sql.find('__BATCH_LOAD_PREDICATES__') == -1 -%}
    {%- set error_message -%}
      Model '{{ model.unique_id }}' does not include the required string '__BATCH_LOAD_PREDICATES__' in its sql
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}

  {%- set boundaries = clickhouse__get_period_boundaries(batch_load_period, batch_load_min_value, batch_load_max_value) | as_native -%}
  {%- set min_value = boundaries['min_value'][0] -%}
  {%- set max_value = boundaries['max_value'][0] -%}
  {%- set num_batches = boundaries['num_batches'][0] | int -%}

  -- commit each batch as a separate transaction
  {% for offset in range(num_batches) -%}
    {%- set msg = "Loading batch " ~ (offset + 1) ~ " of " ~ (num_batches) -%}
    {{ print(msg) }}

    {%- set filtered_sql = clickhouse__get_period_sql(sql, batch_load_column, batch_load_period, min_value, max_value, offset) -%}

    {% call statement('main') %}
      {% if offset == 0 %}
        {{ get_create_table_as_sql(temporary, relation, filtered_sql) }}
      {% else %}
        {{ clickhouse__insert_into(relation, filtered_sql) }}
      {% endif %}
    {% endcall %}
  {% endfor %}
{% endmacro %}
