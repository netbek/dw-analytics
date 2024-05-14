{% macro generate_predicates() %}
    {%- set materialized = config.get('materialized') -%}
    {%- set range_column = config.get('range_column') -%}
    {%- set range_min = config.get('range_min') -%}
    {%- set range_max = config.get('range_max') -%}
    {%- set batch_type = config.get('batch_type') -%}
    {%- set incremental_source_columns = config.get('incremental_source_columns') -%}
    {%- set incremental_target_column = config.get('incremental_target_column') -%}
    {%- set incremental_operator = config.get('incremental_operator', 'or') -%}

    {% if materialized == 'incremental' and batch_type and not is_incremental() %}
        {# Add placeholder for batch predicates #}
        __BATCH_PREDICATES__
    {% else %}
        true
    {% endif %}

    {% if model['columns'] %}
        {# If range_column variable is set, then override model config #}
        {% if var('range_column', none) %}
            {%- set range_column %}{{ var('range_column', none) }}{% endset -%}
            {%- set range_min %}{{ var('range_min', none) }}{% endset -%}
            {%- set range_max %}{{ var('range_max', none) }}{% endset -%}
        {% endif %}

        {# Restrict full and incremental refresh to given range in source database #}
        {% if range_column %}
            {%- if range_min or range_max -%}
                {%- set data_type = model['columns'][range_column]['data_type'] -%}
                {%- set meta = model['columns'][range_column].get('meta', {}) -%}
                {%- set min_value = meta.get('min_value') -%}

                {%- if 'DateTime' in data_type -%}
                    {%- if range_min -%}
                        {%- if min_value is not none -%}
                            and {{ range_column }} >= greatest(toDateTime64('{{ min_value }}', 6), toDateTime64('{{ range_min }}', 6))
                        {%- else -%}
                            and {{ range_column }} >= toDateTime64('{{ range_min }}', 6)
                        {%- endif -%}
                    {%- endif -%}

                    {%- if range_max %}
                        and {{ range_column }} <= least(toDateTime64(current_date(), 6), toDateTime64('{{ range_max }}', 6)) + interval '1 day' - interval '1 microsecond'
                    {%- endif -%}
                {%- else -%}
                    {{ exceptions.raise_compiler_error('"' ~ data_type ~ '" data type is not supported') }}
                {%- endif -%}
            {%- else -%}
                {{ exceptions.raise_compiler_error('"range_min" or "range_max" is required because "range_column" is set') }}
            {%- endif -%}
        {% endif %}
    {% endif %}

    {% if materialized == 'incremental' and incremental_source_columns and is_incremental() %}
        {# Restrict incremental refresh to range greater than maximum value in target database #}
        and (
            {%- for source_column in incremental_source_columns -%}
                {%- set target_column = incremental_target_column if incremental_target_column is not none else source_column -%}
                {%- set data_type = model['columns'][source_column]['data_type'] %}
                {% if not loop.first -%}{{ incremental_operator }} {%- endif %}
                {{ source_column }} > (
                    {# For ClickHouse >= v24.3 #}
                    {%- if 'DateTime' in data_type -%}
                        select toString(max({{ target_column }})) from {{ this }}
                    {%- else -%}
                        select max({{ target_column }}) from {{ this }}
                    {%- endif -%}
                )
            {%- endfor -%}
        )
    {% endif %}
{% endmacro %}
