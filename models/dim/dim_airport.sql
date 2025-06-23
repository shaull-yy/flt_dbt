
{{
	config(
		   uniqu_key = 'airport_code',
		   pre_hook = "{{log_model('start')}}",
		   post_hook = ["{{log_model('end')}}", 
		   "{% if not is_incremental() %} insert into {{ this }} (airport_code, airport_name_en, airport_name_ru, airport_city) values (-1,'NA', 'NA', 'NA') {% endif %}"]
		  )
}} 


select
	airport_code
	,ad.airport_name ->> 'en' as airport_name_en
	,ad.airport_name ->> 'ru' as airport_name_ru
	,ad.city ->> 'en' as airport_city
	,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC'  as etl_time_utc
FROM {{ source('stg', 'airports_data') }} as ad