{% macro batch_load_filter() %}
    {% if config.get('materialized') == 'incremental' and config.get('batch_load_type') and not is_incremental() %}
        __BATCH_LOAD_FILTER__
    {% else %}
        true
    {% endif %}
{% endmacro %}
