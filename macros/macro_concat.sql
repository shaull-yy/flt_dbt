{% macro macro_concat(col_a, col_b) %}
  {{col_a}} || '-' || {{col_b}}
{% endmacro %}