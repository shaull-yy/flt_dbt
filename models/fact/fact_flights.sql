{{
  config(
	unique_key = 'flight_id',
	pre_hook = "{{log_model('start')}}",
	post_hook = ["{{manual_refresh(this)}}", "{{log_model('end')}}"]
	)
}}

with manual_ref_dt as(
select z.from_date
from {{target.schema}}.z_refresh_from as z
where z.to_refresh = 1
  and z.table_name = '{{this}}'
),
{% if is_incremental() %}
null_rows as (
select stg_flt_minu1.*
from {{this}} as fact_flt
inner join {{ source('stg', 'flights') }} as stg_flt_minu1 on fact_flt.flight_id = stg_flt_minu1.flight_id
where 
	fact_flt.aircraft_code = '-1' 
	or fact_flt.arrival_airport = '-1'
	or fact_flt.departure_airport = '-1'
),
{% endif %}
stg_flt_tb as (
select
	stg_flt.*
from {{ source('stg', 'flights') }} as stg_flt
{% if is_incremental() %}
left join manual_ref_dt as manual_ref_dt on 1 = 1
where 1 = 1
	and stg_flt.last_update > COALESCE(
		manual_ref_dt.from_date
		,(select max(last_update) from {{this}})
		,'{{ var('init_date') }}'
	)
{% endif %}
),
combined_stg_flt as (
select	stg_flt_tb.* from stg_flt_tb
{% if is_incremental() %}
union
select null_rows.* from null_rows
{% endif %}
),
calc_data as (
select
	f.flight_id, f.flight_no, f.scheduled_departure, f.scheduled_arrival, f.status, f.actual_departure, f.actual_arrival, f.last_update
    ,round(extract(epoch from f.scheduled_arrival - f.scheduled_departure)/3600,2)  as flt_duration_expected_hr
    ,round(extract(epoch from f.actual_arrival - f.actual_departure)/3600,2)  as flt_duration_actual_hr
    ,case when air_crft.aircraft_code is null then '-1'
         else f.aircraft_code
    end as aircraft_code
    ,case when arrive_port.airport_code is null then '-1'
         else f.arrival_airport
    end as arrival_airport
    ,case when depart_port.airport_code is null then '-1'
         else f.departure_airport
    end as departure_airport
from combined_stg_flt as f
left join {{ source('stg', 'aircrafts_data') }} as air_crft    on f.aircraft_code = air_crft.aircraft_code
left join {{ source('stg', 'airports_data') }}  as arrive_port on arrive_port.airport_code = f.arrival_airport
left join {{ source('stg', 'airports_data') }}  as depart_port on depart_port.airport_code = f.departure_airport
)
select 
	f2.*
    ,case when f2.flt_duration_actual_hr is null then 'NA'
	      when f2.flt_duration_expected_hr < f2.flt_duration_actual_hr then 'longer'
          when f2.flt_duration_expected_hr = f2.flt_duration_actual_hr then 'as expected'
          when f2.flt_duration_expected_hr < f2.flt_duration_actual_hr then 'shorter'
     end as flight_duration_ind
	,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC'  as etl_time_utc
from calc_data as f2