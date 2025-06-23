
{{
	config(
		   uniqu_key = 'aircraft_code',
		   pre_hook = "{{log_model('start')}}",
		   post_hook = ["{{log_model('end')}}", 
		   "{% if not is_incremental() %} insert into {{ this }} (aircraft_code, model_en, model_ru, flight_range, seat_no, fare_conditions) values (-1,'NA', 'NA', 'NA','NA', 'NA') {% endif %}"]
		  )
}} 


SELECT 
	ac.aircraft_code
	, ac.model ->> 'en' as model_en
	, ac.model ->> 'ru' as model_ru
	, ac."range"
	,case when ac."range" > 5600 then 'high'
		  else 'low'
	end as flight_range
	,seat_no
	, fare_conditions
	, ac.last_update
	,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC'  as etl_time_utc
FROM {{ source('stg', 'aircrafts_data') }} ac
left join {{ source('stg', 'seats') }} as s  on ac.aircraft_code = s.aircraft_code
