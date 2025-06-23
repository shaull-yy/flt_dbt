{% macro macro_clean_string(column_name) %}
  lower(regexp_replace({{ column_name }}, '[^a-zA-Z0-9 ]', '', 'g'))
{% endmacro %}