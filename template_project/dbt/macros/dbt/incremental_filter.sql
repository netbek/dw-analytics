{% macro incremental_filter(time_column='modified_at') %}
    {% if is_incremental() %}
        {# <= 24.2 #}
        {# {{ time_column }} > (select max({{ time_column }}) from {{ this }}) #}
        {# >= 24.3 #}
        {{ time_column }} > (select toString(max({{ time_column }})) from {{ this }})
    {% else %}
        true
    {% endif %}
{% endmacro %}
