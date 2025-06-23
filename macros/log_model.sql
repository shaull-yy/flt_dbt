{% macro log_model(start_end) %}

{% if start_end == 'start' %}
  insert into {{target.schema}}.dbt_log (id, model, start_date, end_date, run_duration, run_dur_sec, etl_date_utc) values('{{ invocation_id}}', '{{this}}', clock_timestamp(), null, null, null, '{{ run_started_at }}'::timestamp);
{%else %}
  update {{target.schema}}.dbt_log set end_date = clock_timestamp(), run_duration = clock_timestamp() - start_date, run_dur_sec = EXTRACT(EPOCH FROM (clock_timestamp() - start_date)) where id = '{{ invocation_id}}' and model = '{{this}}';
{% endif %}

{% endmacro %}