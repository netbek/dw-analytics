{% macro incremental_filter(watermark_column='modified_at') %}
    {% if is_incremental() %}
        {# <= 24.2 #}
        {# {{ watermark_column }} > (select max({{ watermark_column }}) from {{ this }}) #}
        {# >= 24.3 #}
        {{ watermark_column }} > (select toString(max({{ watermark_column }})) from {{ this }})
    {% else %}
        true
    {% endif %}
{% endmacro %}
