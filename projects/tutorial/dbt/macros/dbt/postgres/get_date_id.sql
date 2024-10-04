{% macro postgres__get_date_id(column) %}
    to_char({{ column }}, 'YYMMDD')::int
{% endmacro %}
