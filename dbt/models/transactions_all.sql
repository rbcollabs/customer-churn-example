select distinct
	msno,
	date(transaction_date,'YYYYMMDD') as transaction_dt,
	date(membership_expire_date,'YYYYMMDD') as expiration_dt,
	TO_TIMESTAMP_NTZ(date_from_parts(date_part(year,expiration_dt),date_part(month,expiration_dt),1)) as ts, --predict churn in the beginning of the  month when account is expiring
	payment_method_id,
	payment_plan_days,
	plan_list_price,
	actual_amount_paid,
	is_auto_renew,
	is_cancel
from {{ source('churn', 'transactions') }}
