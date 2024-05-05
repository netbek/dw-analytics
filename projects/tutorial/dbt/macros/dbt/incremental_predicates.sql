{% macro incremental_predicates(source_watermark_columns, target_watermark_column=None, operator='or') %}
    {% if is_incremental() %}
        (
            {% for source_watermark_column in source_watermark_columns %}
                {% set column = target_watermark_column if target_watermark_column is not none else source_watermark_column %}
                {% set data_type = model['columns'][source_watermark_column]['data_type'] %}
                {% if not loop.first %}{{ operator }} {% endif %}
                {{ source_watermark_column }} > (
                    {# For ClickHouse >= v24.3 #}
                    {%- if 'DateTime' in data_type -%}
                        select toString(max({{ column }})) from {{ this }}
                    {%- else -%}
                        select max({{ column }}) from {{ this }}
                    {%- endif -%}
                )
            {% endfor %}
        )
    {% else %}
        true
    {% endif %}
{% endmacro %}
