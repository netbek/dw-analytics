{% macro upload_run_results(results) -%}
    {% if execute %}
        {% for resource_type in ['model', 'seed'] %}
            {% set objects = results | selectattr('node.resource_type', 'equalto', resource_type) | list %}
            {% set relation = get_relation('dbt_' ~ resource_type ~ '_run_results') %}
            {% set columns = adapter.get_columns_in_relation(relation) %}
            {% set columns_dict = {} %}
            {% set column_names = [] %}
            {% for column in columns %}
                {% do columns_dict.update({column.name: column}) %}
                {% do column_names.append(column.name) %}
            {% endfor %}

            {% if objects and columns | length %}
                {% set values -%}
                    {% for object in objects %}
                        {%- set run_result_dict = get_run_result_dict(
                            object, flags, invocation_id, run_started_at
                        ) -%}
                        (
                            {% for column_name, column in columns_dict.items() %}
                                {%- set data_type = column.data_type -%}
                                {%- set value = run_result_dict.get(column_name) -%}

                                {%- if column_name == 'id' -%}
                                    cityHash64(
                                        {%- for part in value -%}
                                            '{{ part }}'
                                            {%- if not loop.last -%},{%- endif -%}
                                        {%- endfor -%}
                                    )
                                {%- elif value is none -%}
                                    null
                                {%- elif 'Float32' in data_type or 'Float64' in data_type -%}
                                    {{ value }}
                                {%- elif 'Int32' in data_type or 'Int64' in data_type -%}
                                    {{ value }}
                                {%- elif 'DateTime64' in data_type -%}
                                    toDateTime64('{{ value.isoformat() }}', 6)
                                {%- elif 'DateTime' in data_type -%}
                                    toDateTime('{{ value.strftime("%Y-%m-%dT%H:%M:%S") }}')
                                {%- else -%}
                                    '{{ value }}'
                                {%- endif -%}

                                {%- if not loop.last %},{% endif %}
                            {%- endfor %}
                        )
                        {%- if not loop.last %},{% endif %}
                    {% endfor %}
                {%- endset %}

                {% set sql %}
                    insert into {{ relation }}
                    ({{ column_names | join(', ') }})
                    values
                    {{ values }}
                {% endset %}

                {% do run_query(sql) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{%- endmacro %}
