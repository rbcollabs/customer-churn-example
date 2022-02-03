{{
  config(
    materialized = 'view',
		meta = {
			"continual": {
				'type': 'FeatureSet',
				'name': 'user_logs',
				'entity': 'kkbox_user',
				'index': 'msno',
				'time_index': 'ts',
				'exclude_columns': ['log_date', 'date'],
				}
		}
  )
}}

select distinct *,
	date(date,'YYYYMMDD') as log_date,
	TO_TIMESTAMP_NTZ(log_date) as ts
from {{ source('churn', 'user_logs') }}
