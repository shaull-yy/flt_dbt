{% macro delete_from() %}

{% if is_incremental() %}
  delete from {{this}}
{% endif %}

{% endmacro %}
