{% macro incremental_predicates(source_watermark_columns, target_watermark_column=None, operator='or') %}
    {% if is_incremental() %}
        (
            {% for source_watermark_column in source_watermark_columns %}
                {% if not loop.first %}{{ operator }} {% endif %}
                {{ source_watermark_column }} > (
                    select toString(max(
                        {%- if target_watermark_column is none -%}
                            {{ source_watermark_column }}
                        {%- else -%}
                            {{ target_watermark_column }}
                        {%- endif -%}
                    )) from {{ this }}
                )
            {% endfor %}
        )
    {% else %}
        true
    {% endif %}
{% endmacro %}
