{#
  Source: https://github.com/dbt-labs/dbt-postgres/blob/v1.8.2/dbt/include/postgres/macros/adapters.sql
  - Added "access_method" config parameter
#}
{% macro postgres__create_table_as(temporary, relation, sql) -%}
  {%- set unlogged = config.get('unlogged', default=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set access_method = config.get('access_method', default=none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary -%}
    temporary
  {%- elif unlogged -%}
    unlogged
  {%- endif %} table {{ relation }}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}
  {% endif -%}
  {% if contract_config.enforced and (not temporary) -%}
      {{ get_table_columns_and_constraints() }} ;
    insert into {{ relation }} (
      {{ adapter.dispatch('get_column_names', 'dbt')() }}
    )
    {%- set sql = get_select_subquery(sql) %}
  {% else %}
    {% if access_method -%}
      using {{ access_method }}
    {%- endif %}
    as
  {% endif %}
  (
    {{ sql }}
  );
{%- endmacro %}
