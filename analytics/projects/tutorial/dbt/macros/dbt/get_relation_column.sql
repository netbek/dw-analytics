{% macro get_relation_column(relation, column_name) %}
    {% if relation %}
        {% set columns = adapter.get_columns_in_relation(relation) | selectattr('name', 'equalto', column_name) -%}
        {% if columns %}
            {% set column = columns | first %}
        {% else %}
            {% set column = none %}
        {% endif %}
    {% else %}
        {% set column = none %}
    {% endif %}
    {{ return(column) }}
{% endmacro %}
