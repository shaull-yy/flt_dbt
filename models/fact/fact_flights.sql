{{
  config(
	unique_key = 'flight_id',
	pre_hook = "{{log_model('start')}}",
	post_hook = ["{{manual_refresh(this)}}", "{{log_model('end')}}"]
	)
}}

with z_refresh_from as(
select z.from_date
from {{target.schema}}.z_refresh_from as z
where z.to_refresh = 1
  and z.table_name = '{{this}}'
),
{% if is_incremental() %}
null_rows as (
select flight_id
from {{this}} 
where 
	aircraft_code = '-1' 
	or arrival_airport = '-1'
	or departure_airport = '-1'
),
{% endif %}
calc_data as (
select
	f.flight_id
	,f.status
    ,round(extract(epoch from f.scheduled_arrival - f.scheduled_departure)/3600,2)  as flt_duration_expected_hr
    ,round(extract(epoch from f.actual_arrival - f.actual_departure)/3600,2)  as flt_duration_actual_hr
    ,case when ac.aircraft_code is null then '-1'
         else ac.aircraft_code
    end as aircraft_code_tmp
    ,case when aparr.airport_code is null then '-1'
         else aparr.airport_code
    end as arrival_airport_tmp
    ,case when apdep.airport_code is null then '-1'
         else apdep.airport_code
    end as departure_airport_tmp
from {{ source('stg', 'flights') }} as f
left join {{ source('stg', 'aircrafts_data') }} as ac  on f.aircraft_code = ac.aircraft_code
left join {{ source('stg', 'airports_data') }}  as aparr  on f.arrival_airport = aparr.airport_code
left join {{ source('stg', 'airports_data') }} as apdep  on f.departure_airport = apdep.airport_code
{% if is_incremental() %}
inner join z_refresh_from as z_refresh_from on 1 = 1
where 1 = 1
	and f.last_update >= COALESCE(
		z_refresh_from.from_date
		,(select max(last_update) from {{this}})
		,'{{ var('init_date') }}'
	)
{% endif %}
),
base_flt as (
select 
	f2.flight_id, f2.flight_no, f2.scheduled_departure, f2.scheduled_arrival, f2.status, f2.actual_departure, f2.actual_arrival, f2.last_update
	,calcd.aircraft_code_tmp as aircraft_code
	,calcd.arrival_airport_tmp as arrival_airport
	,calcd.departure_airport_tmp as departure_airport
    ,calcd.flt_duration_expected_hr
    ,calcd.flt_duration_actual_hr
    ,case when flt_duration_actual_hr is null then 'NA'
	      when calcd.flt_duration_expected_hr < calcd.flt_duration_actual_hr then 'longer'
          when calcd.flt_duration_expected_hr = calcd.flt_duration_actual_hr then 'as expected'
          when calcd.flt_duration_expected_hr < calcd.flt_duration_actual_hr then 'shorter'
     end as flight_duration_ind
	,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC'  as etl_time_utc
from {{ source('stg', 'flights') }} as f2
inner join calc_data as calcd  on f2.flight_id = calcd.flight_id
)
select
	bf1.*
from base_flt as bf1
{% if is_incremental() %}
union
select 
	bf2.*
from base_flt as bf2
inner join null_rows as null_rows  on bf2.flight_id = null_rows.flight_id
{% endif %}