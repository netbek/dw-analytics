{% macro batch_load_predicates() %}
    {% if config.get('materialized') == 'incremental' and config.get('batch_load_type') and not is_incremental() %}
        __BATCH_LOAD_PREDICATES__
    {% else %}
        true
    {% endif %}
{% endmacro %}
