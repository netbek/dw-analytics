{% macro get_run_result_dict(run_result, flags, invocation_id, run_started_at) -%}
    {% set is_full_refresh = run_result.node.config.full_refresh %}
    {% if is_full_refresh is none %}
        {% set is_full_refresh = flags.FULL_REFRESH %}
    {% endif %}
    {% set run_type = 'full' if is_full_refresh else 'incremental' %}
    {% set compile_start_time = (run_result.timing | selectattr('name', 'eq', 'compile') | first | default({}))['started_at'] or none %}
    {% set query_end_time = (run_result.timing | selectattr('name', 'eq', 'execute') | first | default({}))['completed_at'] or none %}
    {% set rows_affected = run_result.adapter_response.get('rows_affected') %}
    {% set status = run_result.status.value if run_result.status else none %}

    {% do return({
        'id': [invocation_id, run_result.node.unique_id],
        'invocation_id': invocation_id,
        'node_id': run_result.node.unique_id,
        'resource_type': run_result.node.resource_type.value,
        'schema': run_result.node.schema,
        'name': run_result.node.name,
        'alias': run_result.node.alias,
        'materialization': run_result.node.config.materialized,
        'run_type': run_type,
        'run_start_time': run_started_at.replace(tzinfo=None),
        'compile_start_time': compile_start_time,
        'compile_end_time': none,
        'query_start_time': none,
        'query_end_time': query_end_time,
        'execution_interval': run_result.execution_time,
        'status': status,
        'message': run_result.message,
        'rows_affected': rows_affected,
        'thread_id': run_result.thread_id,
        'adapter_response': tojson(run_result.adapter_response),
    }) %}
{%- endmacro %}
