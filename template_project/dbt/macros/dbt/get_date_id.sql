{% macro get_date_id(column) %}
    {{ adapter.dispatch('get_date_id', 'default')(column) }}
{% endmacro %}
