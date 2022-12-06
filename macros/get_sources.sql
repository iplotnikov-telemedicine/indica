{% macro get_sources(source_prefix, table_name) %}
    {% set sources = [] %}
    {% if execute %}
    {% for node_name, node in graph.nodes.items() %}
        {% if node.resource_type == 'source' and node.name == table_name and node.source_name.startswith(source_prefix) %}
            {% set new_source = source(node.source_name, node.name) %}
            {% do sources.append(new_source) %}
        {% endif %}
    {% endfor %}
    {% do return(sources) %}
    {% endif %}
{% endmacro %}