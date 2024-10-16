{# https://clickhouse.com/docs/en/sql-reference/table-functions/postgresql #}
{% macro postgresql(source_name, table_name) %}
    postgresql(
        '{{ env_var('SOURCE_POSTGRES_HOST') }}:{{ env_var('SOURCE_POSTGRES_PORT') }}',
        '{{ env_var('SOURCE_POSTGRES_DATABASE') }}',
        '{{ table_name }}',
        '{{ env_var('SOURCE_POSTGRES_USERNAME') }}',
        '{{ env_var('SOURCE_POSTGRES_PASSWORD') }}',
        '{{ env_var('SOURCE_POSTGRES_SCHEMA') }}'
    )
{% endmacro %}
