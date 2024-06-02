{% macro get_run_result_dict(run_result, flags, invocation_id, run_started_at) -%}
    {% set is_full_refresh = run_result.node.config.full_refresh %}
    {% if is_full_refresh is none %}
        {% set is_full_refresh = flags.FULL_REFRESH %}
    {% endif %}
    {% set run_type = 'full' if is_full_refresh else 'incremental' %}

    {% set compile_timing = none %}
    {% set execute_timing = none %}
    {% if run_result.timing %}
        {% set compile_timing = run_result.timing | selectattr('name', 'eq', 'compile') | first %}
        {% set execute_timing = run_result.timing | selectattr('name', 'eq', 'execute') | first %}
    {% endif %}

    {% set compile_timing_dict = get_timing_dict(compile_timing) %}
    {% set execute_timing_dict = get_timing_dict(execute_timing) %}

    {% set rows_affected = run_result.adapter_response.get('rows_affected') %}
    {% set status = run_result.status.value if run_result.status else none %}

    {% do return({
        'invocation_id': invocation_id,
        'node_id': run_result.node.unique_id,
        'thread_id': run_result.thread_id,
        'resource_type': run_result.node.resource_type.value,
        'schema': run_result.node.schema,
        'name': run_result.node.name,
        'alias': run_result.node.alias,
        'materialization': run_result.node.config.materialized,
        'run_type': run_type,
        'run_start_time': run_started_at.replace(tzinfo=None),
        'compile_start_time': compile_timing_dict['start_time'],
        'compile_end_time': compile_timing_dict['end_time'],
        'compile_duration': compile_timing_dict['duration'],
        'execute_start_time': execute_timing_dict['start_time'],
        'execute_end_time': execute_timing_dict['end_time'],
        'execute_duration': execute_timing_dict['duration'],
        'status': status,
        'message': run_result.message,
        'rows_affected': rows_affected,
        'adapter_response': tojson(run_result.adapter_response),
    }) %}
{%- endmacro %}

{% macro get_timing_dict(timing) -%}
    {% set start_time = none %}
    {% set end_time = none %}
    {% set duration = none %}

    {% if timing %}
        {% set start_time = timing.started_at %}
        {% set end_time = timing.completed_at %}
    {% endif %}

    {% if start_time and end_time %}
        {% set duration = ((end_time - start_time).total_seconds() * 1000) | round %}
    {% endif %}

    {{ return({
        'start_time': start_time,
        'end_time': end_time,
        'duration': duration
    }) }}
{%- endmacro %}
