{% macro get_date_id(column) %}
    {{ adapter.dispatch('get_date_id')(column) }}
{% endmacro %}


{% macro clickhouse__get_date_id(column) %}
    toInt32(formatDateTime({{ column }}, '%Y%m%d'))
{% endmacro %}


{% macro postgres__get_date_id(column) %}
    to_char({{ column }}, 'YYMMDD')::int
{% endmacro %}
