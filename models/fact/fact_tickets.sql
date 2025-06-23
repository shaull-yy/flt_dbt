{{
  config(
	unique_key = 'book_ref',
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
	b.*
	,t.ticket_no, t.passenger_id, t.passenger_name, t.last_update
	,t.contact_data ->> 'email' as cust_mail_address
	,t.contact_data ->> 'phone' as cust_phone_num
	,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC'  as etl_time_utc
from flt_stg.bookings as b
left join flt_stg.tickets as t  on b.book_ref = t.book_ref
{% if is_incremental() %}
inner join z_refresh_from as z_refresh_from on 1 = 1
where 1 = 1
	and t.last_update >= COALESCE
	(
	z_refresh_from.from_date
	,(select max(last_update) from {{this}})
	,'{{ var('init_date') }}'
	)

{% endif %}