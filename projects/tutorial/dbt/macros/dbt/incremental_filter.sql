{% macro incremental_filter(watermark_column='modified_at') %}
    {% if is_incremental() %}
        {{ watermark_column }} > (select max({{ watermark_column }}) from {{ this }})
    {% else %}
        true
    {% endif %}
{% endmacro %}
