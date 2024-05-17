{% macro get_date_id(column) %}
    toInt32(formatDateTime({{ column }}, '%Y%m%d'))
{% endmacro %}
