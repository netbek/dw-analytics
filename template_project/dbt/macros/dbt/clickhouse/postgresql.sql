{# Source: https://clickhouse.com/docs/en/sql-reference/table-functions/postgresql #}
{% macro postgresql(source_name, table_name) %}
    postgresql(
        '{{ env_var('POSTGRES_HOST') }}:{{ env_var('POSTGRES_PORT') }}',
        '{{ env_var('POSTGRES_DATABASE') }}',
        '{{ table_name }}',
        '{{ env_var('POSTGRES_USERNAME') }}',
        '{{ env_var('POSTGRES_PASSWORD') }}',
        '{{ env_var('POSTGRES_SCHEMA') }}'
    )
{% endmacro %}
