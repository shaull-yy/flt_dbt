{% macro coalesces_id(tb_alias, col_name, as_prefix) %}

coalesce({{tb_alias}}.{{col_name}}, -1) as {{as_prefix}}_{{ col_name }}

{% endmacro %}