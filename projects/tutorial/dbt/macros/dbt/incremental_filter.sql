{% macro incremental_filter(time_column='modified_at') %}
    {% if is_incremental() %}
        {{ time_column }} > (select max({{ time_column }}) from {{ this }})
    {% else %}
        true
    {% endif %}
{% endmacro %}
