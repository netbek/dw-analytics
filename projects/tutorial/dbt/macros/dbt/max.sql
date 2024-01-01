{% macro max(values) %}{{ values|select('defined')|list|max }}{% endmacro %}
