{% macro get_node(graph, name, resource_type='model') %}
    {% if not execute %}
        {{ return(none) }}
    {% else %}
        {% set nodes = graph.nodes.values()
            | selectattr('resource_type', 'equalto', resource_type)
            | selectattr('package_name', 'equalto', project_name)
            | selectattr('name', 'equalto', name) %}

        {% if nodes %}
            {{ return(nodes | first) }}
        {% else %}
            {{ return(none) }}
        {% endif %}
    {% endif %}
{% endmacro %}
