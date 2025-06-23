{{
  config(
	unique_key = 'ticket_no',
	pre_hook = "{{log_model('start')}}",
	post_hook = ["{{manual_refresh(this)}}", "{{log_model('end')}}"]
	)
}}

with z_refresh_from as(
select z.from_date
from {{target.schema}}.z_refresh_from as z
where z.to_refresh = 1
  and z.table_name = '{{this}}'
)
select
bp.*
,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC'  as etl_time_utc
from {{ source('stg', 'boarding_passes') }} as bp
{% if is_incremental() %}
inner join z_refresh_from as z_refresh_from on 1 = 1
where 1 = 1
	and bp.last_update >= COALESCE(
		z_refresh_from.from_date
		,(select max(last_update) from {{this}})
		,'{{ var('init_date') }}'
	)
{% endif %}