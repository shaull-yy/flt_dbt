{% macro macro_example(col_a, col_b) %}
  {{col_a}} || '-' || {{col_b}}
{% endmacro %}