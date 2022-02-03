{{
  config(
    materialized = 'view',
		meta = {
			"continual": {
				'type': 'FeatureSet',
				'name': 'user_info',
				'entity': 'kkbox_user',
				'index': 'msno',
				'columns': [
					{"name" : "city", "type": "CATEGORICAL"},
					{"name" : "registered_via", "type": "CATEGORICAL"},
				],
				}
		}
  )
}}

 select
 	msno,
	city,
	bd,
	gender,
	registered_via,
	date(registration_init_time,'YYYYMMDD') as registration_dt
from {{ source('churn', 'members') }}
