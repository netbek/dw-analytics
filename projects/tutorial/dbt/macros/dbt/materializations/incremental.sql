{# Adapted from https://github.com/ClickHouse/dbt-clickhouse/blob/v1.6.2/dbt/include/clickhouse/macros/materializations/incremental/incremental.sql #}
{% materialization incremental, adapter='clickhouse' %}

  {%- set batch_load_type = config.get('batch_load_type') -%}
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
    {% if batch_load_type == 'period' %}
      {{ clickhouse__batch_load_period_create_table(False, target_relation, sql) }}
    {% elif batch_load_type == 'sequence' %}
      {{ clickhouse__batch_load_sequence_create_table(False, target_relation, sql) }}
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
    {% if batch_load_type == 'period' %}
      {{ clickhouse__batch_load_period_create_table(False, intermediate_relation, sql) }}
    {% elif batch_load_type == 'sequence' %}
      {{ clickhouse__batch_load_sequence_create_table(False, intermediate_relation, sql) }}
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


{% macro clickhouse__get_batch_load_min_max(model, column) -%}
  {% set sql %}
    select min({{ column }}) as min, max({{ column }}) as max
    from {{ model }}
  {% endset %}
  {{ return(dbt_utils.get_query_results_as_dict(sql)) }}
{%- endmacro %}


{# Adapted from https://github.com/dbt-labs/dbt-labs-experimental-features/blob/42f36a4418dd4f7f6b0bd36c03dcc3ec01bb3304/insert_by_period/macros/get_period_boundaries.sql #}
{% macro clickhouse__get_period_boundaries(period, start_value, end_value) -%}
  {% set sql %}
    with data as (
      select
        toDateTime64('{{ start_value|trim }}', 6) as start_value,
        {% if end_value %}toDateTime64('{{ end_value|trim }}', 6) + interval '1 day' - interval '1 microsecond'{% else %}toDateTime64({{ dbt.current_timestamp() }}, 6){% endif %} as end_value
    )

    select
      start_value,
      end_value,
      {{ datediff('start_value', 'end_value', period) }} + 1 as num_batches
    from data
  {% endset %}
  {{ return(dbt_utils.get_query_results_as_dict(sql)) }}
{%- endmacro %}


{# Adapted from https://github.com/dbt-labs/dbt-labs-experimental-features/blob/42f36a4418dd4f7f6b0bd36c03dcc3ec01bb3304/insert_by_period/macros/get_period_sql.sql #}
{% macro clickhouse__get_period_sql(sql, column, period, start_value, end_value, offset) -%}
  {%- set batch_load_predicates -%}
    (
      "{{ column }}" >= toDateTime64('{{ start_value }}', 6) + interval '{{ offset }} {{ period }}' and
      "{{ column }}" <  toDateTime64('{{ start_value }}', 6) + interval '{{ offset + 1 }} {{ period }}' and
      "{{ column }}" <= toDateTime64('{{ end_value }}', 6)
    )
  {%- endset -%}
  {%- set filtered_sql = sql | replace('__BATCH_LOAD_PREDICATES__', batch_load_predicates) -%}
  {{ return(filtered_sql) }}
{%- endmacro %}


{# Adapted from https://github.com/dbt-labs/dbt-labs-experimental-features/blob/42f36a4418dd4f7f6b0bd36c03dcc3ec01bb3304/insert_by_period/macros/insert_by_period_materialization.sql #}
{% macro clickhouse__batch_load_period_create_table(temporary, relation, sql) %}
  {%- set batch_load_period = config.get('batch_load_period', 'week') -%}
  {%- set batch_load_column = config.require('batch_load_column') -%}
  {%- set batch_load_start = config.require('batch_load_start') -%}
  {%- set batch_load_end = config.get('batch_load_end') -%}

  {%- if sql.find('__BATCH_LOAD_PREDICATES__') == -1 -%}
    {%- set error_message -%}
      Model '{{ model.unique_id }}' does not include the required string '__BATCH_LOAD_PREDICATES__' in its sql
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}

  {%- set boundaries = clickhouse__get_period_boundaries(batch_load_period, batch_load_start, batch_load_end) | as_native -%}
  {%- set start_value = boundaries['start_value'][0] -%}
  {%- set end_value = boundaries['end_value'][0] -%}
  {%- set num_batches = boundaries['num_batches'][0] | int -%}

  -- commit each batch as a separate transaction
  {% for offset in range(num_batches) -%}
    {%- set msg = "Loading batch " ~ (offset + 1) ~ " of " ~ (num_batches) -%}
    {{ print(msg) }}

    {%- set filtered_sql = clickhouse__get_period_sql(sql, batch_load_column, batch_load_period, start_value, end_value, offset) -%}

    {% call statement('main') %}
      {% if offset == 0 %}
        {{ get_create_table_as_sql(temporary, relation, filtered_sql) }}
      {% else %}
        {{ clickhouse__insert_into(relation, filtered_sql) }}
      {% endif %}
    {% endcall %}
  {% endfor %}
{% endmacro %}


{% macro clickhouse__get_sequence_boundaries(batch_size, start_value, end_value) -%}
  {% set sql %}
    select
      {{ start_value }} as start_value,
      {{ end_value }} as end_value,
      ceil(({{ end_value }} - {{ start_value }})::real / {{ batch_size }})::integer as num_batches
  {% endset %}
  {{ return(dbt_utils.get_query_results_as_dict(sql)) }}
{%- endmacro %}


{% macro clickhouse__get_sequence_sql(sql, column, batch_size, start_value, end_value, offset) -%}
  {%- set batch_load_predicates -%}
    (
      "{{ column }}" >= {{ start_value }} + ({{ batch_size }} * {{ offset }}) and
      "{{ column }}" <  {{ start_value }} + ({{ batch_size }} * {{ offset + 1 }}) and
      "{{ column }}" <= {{ end_value }}
    )
  {%- endset -%}
  {%- set filtered_sql = sql | replace('__BATCH_LOAD_PREDICATES__', batch_load_predicates) -%}
  {{ return(filtered_sql) }}
{%- endmacro %}


{% macro clickhouse__batch_load_sequence_create_table(temporary, relation, sql) %}
  {%- set batch_load_size = config.get('batch_load_size', 1000) | int -%}
  {%- set batch_load_column = config.require('batch_load_column') -%}
  {%- set batch_load_source_model = config.get('batch_load_source_model') -%}

  {%- if batch_load_source_model -%}
    {%- set min_max = clickhouse__get_batch_load_min_max(batch_load_source_model, batch_load_column) | as_native -%}
    {%- set batch_load_start = min_max['min'][0] | int -%}
    {%- set batch_load_end = min_max['max'][0] | int -%}
  {%- else -%}
    {%- set batch_load_start = config.require('batch_load_start') -%}
    {%- set batch_load_end = config.get('batch_load_end') -%}
  {%- endif -%}

  {%- if sql.find('__BATCH_LOAD_PREDICATES__') == -1 -%}
    {%- set error_message -%}
      Model '{{ model.unique_id }}' does not include the required string '__BATCH_LOAD_PREDICATES__' in its sql
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}

  {%- set boundaries = clickhouse__get_sequence_boundaries(batch_load_size, batch_load_start, batch_load_end) | as_native -%}
  {%- set start_value = boundaries['start_value'][0] | int -%}
  {%- set end_value = boundaries['end_value'][0] | int -%}
  {%- set num_batches = boundaries['num_batches'][0] | int -%}

  -- commit each batch as a separate transaction
  {% for offset in range(num_batches) -%}
    {%- set msg = "Loading batch " ~ (offset + 1) ~ " of " ~ (num_batches) -%}
    {{ print(msg) }}

    {%- set filtered_sql = clickhouse__get_sequence_sql(sql, batch_load_column, batch_load_size, start_value, end_value, offset) -%}

    {% call statement('main') %}
      {% if offset == 0 %}
        {{ get_create_table_as_sql(temporary, relation, filtered_sql) }}
      {% else %}
        {{ clickhouse__insert_into(relation, filtered_sql) }}
      {% endif %}
    {% endcall %}
  {% endfor %}
{% endmacro %}
