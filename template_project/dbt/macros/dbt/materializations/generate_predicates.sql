{% macro generate_predicates() %}
    {%- set range_column = config.get('range_column') -%}
    {%- set range_from = config.get('range_from') -%}
    {%- set range_to = config.get('range_to') -%}
    {%- set incremental_source_columns = config.get('incremental_source_columns') -%}
    {%- set incremental_target_column = config.get('incremental_target_column') -%}
    {%- set incremental_operator = config.get('incremental_operator', 'or') -%}

    {% if config.get('materialized') == 'incremental' and config.get('batch_load_type') and not is_incremental() %}
        __BATCH_LOAD_PREDICATES__
    {% else %}
        true
    {% endif %}

    {% if model['columns'] %}
        {# If range_column variable is set, then override macro arguments #}
        {% if var('range_column', '') %}
            {%- set range_column %}{{ var('range_column', '') }}{% endset -%}
            {%- set range_from %}{{ var('range_from', '') }}{% endset -%}
            {%- set range_to %}{{ var('range_to', '') }}{% endset -%}
        {% endif %}

        {# Restrict full and incremental refresh to given range in source database #}
        {% if range_column %}
            {%- if range_from or range_to -%}
                {%- set data_type = model['columns'][range_column]['data_type'] -%}
                {%- set meta = model['columns'][range_column].get('meta', {}) -%}
                {%- set min_value = meta.get('min_value') -%}

                {%- if 'DateTime' in data_type -%}
                    {%- if range_from -%}
                        {%- if min_value -%}
                            and created_at >= greatest(toDateTime64('{{ min_value }}', 6), toDateTime64('{{ range_from }}', 6))
                        {%- else -%}
                            and created_at >= toDateTime64('{{ range_from }}', 6)
                        {%- endif -%}
                    {%- endif -%}

                    {%- if range_to %}
                        and created_at <= least(toDateTime64(current_date(), 6), toDateTime64('{{ range_to }}', 6)) + interval '1 day' - interval '1 microsecond'
                    {%- endif -%}
                {%- else -%}
                    {{ exceptions.raise_compiler_error('"' ~ data_type ~ '" data type is not supported') }}
                {%- endif -%}
            {%- else -%}
                {{ exceptions.raise_compiler_error('"range_from" or "range_to" is required because "range_column" is set') }}
            {%- endif -%}
        {% endif %}

        {# Restrict incremental refresh to range greater than maximum value in target database #}
        {% if is_incremental() and incremental_source_columns %}
            and (
                {%- for source_column in incremental_source_columns -%}
                    {%- set column = incremental_target_column if incremental_target_column is not none else source_column -%}
                    {%- set data_type = model['columns'][source_column]['data_type'] %}
                    {% if not loop.first -%}{{ incremental_operator }} {%- endif %}
                    {{ source_column }} > (
                        {# For ClickHouse >= v24.3 #}
                        {%- if 'DateTime' in data_type -%}
                            select toString(max({{ column }})) from {{ this }}
                        {%- else -%}
                            select max({{ column }}) from {{ this }}
                        {%- endif -%}
                    )
                {%- endfor -%}
            )
        {% endif %}
    {% endif %}
{% endmacro %}
