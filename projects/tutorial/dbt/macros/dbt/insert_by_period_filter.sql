{% macro insert_by_period_filter() %}
    {% if config.get('materialized') == 'incremental' and config.get('insert_by_period__watermark_column') and not is_incremental() %}
        __INSERT_BY_PERIOD_FILTER__
    {% else %}
        true
    {% endif %}
{% endmacro %}
