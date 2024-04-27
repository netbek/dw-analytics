{# Adapted from https://github.com/ClickHouse/dbt-clickhouse/blob/v1.6.2/dbt/include/clickhouse/macros/materializations/incremental/incremental.sql #}
{% materialization incremental, adapter='clickhouse' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}

  {%- set unique_key = config.get('unique_key') -%}
  {% if unique_key is not none and unique_key|length == 0 %}
    {% set unique_key = none %}
  {% endif %}
  {% if unique_key is iterable and (unique_key is not string and unique_key is not mapping) %}
     {% set unique_key = unique_key|join(', ') %}
  {% endif %}
  {%- set inserts_only = config.get('inserts_only') -%}
  {%- set grant_config = config.get('grants') -%}
  {%- set full_refresh_mode = (should_full_refresh() or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}

  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% set to_drop = [] %}

  {% if existing_relation is none %}
    -- No existing table, simply create a new one
    {# {% call statement('main') %}
        {{ get_create_table_as_sql(False, target_relation, sql) }}
    {% endcall %} #}

    {# See original logic above #}
    {% if config.get('insert_by_period__watermark_column') %}
      {{ clickhouse__insert_by_period_create_table(False, target_relation, sql) }}
    {% else %}
      {% call statement('main') %}
        {{ get_create_table_as_sql(False, target_relation, sql) }}
      {% endcall %}
    {% endif %}

  {% elif full_refresh_mode %}
    -- Completely replacing the old table, so create a temporary table and then swap it
    {# {% call statement('main') %}
        {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
    {% endcall %} #}

    {# See original logic above #}
    {% if config.get('insert_by_period__watermark_column') %}
      {{ clickhouse__insert_by_period_create_table(False, intermediate_relation, sql) }}
    {% else %}
      {% call statement('main') %}
        {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
      {% endcall %}
    {% endif %}
    {% set need_swap = true %}

  {% elif inserts_only or unique_key is none -%}
    -- There are no updates/deletes or duplicate keys are allowed.  Simply add all of the new rows to the existing
    -- table. It is the user's responsibility to avoid duplicates.  Note that "inserts_only" is a ClickHouse adapter
    -- specific configurable that is used to avoid creating an expensive intermediate table.
    {% call statement('main') %}
        {{ clickhouse__insert_into(target_relation, sql) }}
    {% endcall %}

  {% else %}
    {% set column_changes = none %}
    {% set incremental_strategy = adapter.calculate_incremental_strategy(config.get('incremental_strategy'))  %}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {%- if on_schema_change != 'ignore' %}
      {%- set column_changes = adapter.check_incremental_schema_changes(on_schema_change, existing_relation, sql) -%}
      {%- if column_changes %}
        {%- if incremental_strategy in ('append', 'delete_insert') %}
          {% set incremental_strategy = 'legacy' %}
          {{ log('Schema changes detected, switching to legacy incremental strategy') }}
        {%- endif %}
      {% endif %}
    {% endif %}
    {% if incremental_strategy != 'delete_insert' and incremental_predicates %}
      {% do exceptions.raise_compiler_error('Cannot apply incremental predicates with ' + incremental_strategy + ' strategy.') %}
    {% endif %}
    {% if incremental_strategy == 'legacy' %}
      {% do clickhouse__incremental_legacy(existing_relation, intermediate_relation, column_changes, unique_key) %}
      {% set need_swap = true %}
    {% elif incremental_strategy == 'delete_insert' %}
      {% do clickhouse__incremental_delete_insert(existing_relation, unique_key, incremental_predicates) %}
    {% elif incremental_strategy == 'append' %}
      {% call statement('main') %}
        {{ clickhouse__insert_into(target_relation, sql) }}
      {% endcall %}
    {% endif %}
  {% endif %}

  {% if need_swap %}
      {% if existing_relation.can_exchange %}
        {% do adapter.rename_relation(intermediate_relation, backup_relation) %}
        {% do exchange_tables_atomic(backup_relation, target_relation) %}
      {% else %}
        {% do adapter.rename_relation(target_relation, backup_relation) %}
        {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% endif %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}


{# Adapted from https://github.com/dbt-labs/dbt-labs-experimental-features/blob/42f36a4418dd4f7f6b0bd36c03dcc3ec01bb3304/insert_by_period/macros/get_period_boundaries.sql #}
{% macro clickhouse__get_period_boundaries(watermark_column, start_date, stop_date, period) -%}
  {% call statement('period_boundaries', fetch_result=True) -%}
    {# with data as (
      select
        date_trunc('second', min('{{ watermark_column }}')) as start_timestamp,
        date_trunc('second', max('{{ watermark_column }}')) + interval '1 second' as stop_timestamp
      from {{ source }}
    ) #}

    with data as (
      select
        date_trunc('second', '{{ start_date|trim }}'::timestamp) as start_timestamp,
        date_trunc('second', {% if stop_date %}'{{ stop_date|trim }}'::timestamp{% else %}{{ dbt.current_timestamp() }}{% endif %}) + interval '1 second' as stop_timestamp
    )

    select
      start_timestamp,
      stop_timestamp,
      {{ datediff('start_timestamp', 'stop_timestamp', period) }}  + 1 as num_periods
    from data
  {%- endcall %}
{%- endmacro %}


{# Adapted from https://github.com/dbt-labs/dbt-labs-experimental-features/blob/42f36a4418dd4f7f6b0bd36c03dcc3ec01bb3304/insert_by_period/macros/get_period_sql.sql #}
{% macro clickhouse__get_period_sql(sql, watermark_column, period, start_timestamp, stop_timestamp, offset) -%}
  {%- set period_filter -%}
    (
      "{{ watermark_column }}" >  '{{ start_timestamp }}'::timestamp + interval '{{ offset }} {{ period }}' and
      "{{ watermark_column }}" <= '{{ start_timestamp }}'::timestamp + interval '{{ offset }} {{ period }}' + interval '1 {{ period }}' and
      "{{ watermark_column }}" <  '{{ stop_timestamp }}'::timestamp
    )
  {%- endset -%}
  {%- set filtered_sql = sql | replace('__INSERT_BY_PERIOD_FILTER__', period_filter) -%}
  {{ return(filtered_sql) }}
{%- endmacro %}


{# Adapted from https://github.com/dbt-labs/dbt-labs-experimental-features/blob/42f36a4418dd4f7f6b0bd36c03dcc3ec01bb3304/insert_by_period/macros/insert_by_period_materialization.sql #}
{% macro clickhouse__insert_by_period_create_table(temporary, relation, sql) %}
  {%- set watermark_column = config.require('insert_by_period__watermark_column') -%}
  {%- set start_date = config.require('insert_by_period__start_date') -%}
  {%- set stop_date = config.get('insert_by_period__stop_date') or '' -%}
  {%- set period = config.get('insert_by_period__period') or 'week' -%}

  {%- if sql.find('__INSERT_BY_PERIOD_FILTER__') == -1 -%}
    {%- set error_message -%}
      Model '{{ model.unique_id }}' does not include the required string '__INSERT_BY_PERIOD_FILTER__' in its sql
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}

  {%- set period_boundaries = clickhouse__get_period_boundaries(watermark_column, start_date, stop_date, period) -%}
  {%- set period_boundaries_results = load_result('period_boundaries')['data'][0] -%}
  {%- set start_timestamp = period_boundaries_results[0] | string -%}
  {%- set stop_timestamp = period_boundaries_results[1] | string -%}
  {%- set num_periods = period_boundaries_results[2] | int -%}

  -- commit each period as a separate transaction
  {% for offset in range(num_periods) -%}
    {%- set msg = "Running for " ~ period ~ " " ~ (offset + 1) ~ " of " ~ (num_periods) -%}
    {{ print(msg) }}

    {%- set filtered_sql = clickhouse__get_period_sql(sql, watermark_column, period, start_timestamp, stop_timestamp, offset) -%}

    {% call statement('main') %}
      {% if offset == 0 %}
        {{ get_create_table_as_sql(temporary, relation, filtered_sql) }}
      {% else %}
        {{ clickhouse__insert_into(relation, filtered_sql) }}
      {% endif %}
    {% endcall %}
  {% endfor %}
{% endmacro %}
