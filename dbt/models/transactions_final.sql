{{
  config(
    materialized = 'view',
		meta = {
			"continual": {
				'type': 'FeatureSet',
				'name': 'user_transactions',
				'entity': 'kkbox_user',
				'index': 'msno',
				'time_index': 'ts',
				'columns': [
					{'name' : 'payment_method_id', 'type': 'CATEGORICAL'},
					{'name' : 'is_auto_renew', 'type': 'BOOLEAN'},
					{'name' : 'most_recent_transaction_cancel', 'type': 'BOOLEAN'},
				],
				'exclude_columns': ['days_expired', 'cutoff_dt'],
				}
		}
  )
}}

with t as (
 select A.*,
  coalesce(B.most_recent_transaction_cancel, 0) as most_recent_transaction_cancel
  from {{ ref('transactions_all') }}  as A
  left join {{ ref('transactions_canceled') }}  as B
  on A.msno = B.msno
  and A.transaction_dt = B.transaction_dt
  where A.is_cancel = 0
  order by A.msno, A.transaction_dt
)
select
  msno, ts,
  max(payment_method_id) as payment_method_id,
  max(payment_plan_days) as payment_plan_days,
  sum(plan_list_price) as plan_list_price,
  sum(actual_amount_paid) as actual_amount_paid,
  max(is_auto_renew) as is_auto_renew,
	max(expiration_dt) as expiration_dt,
	max(transaction_dt) as transaction_dt,
  max(most_recent_transaction_cancel) as most_recent_transaction_cancel
  from t
  group by msno, ts
	--squash sub-montly subs all into one month, use last value for exp/trans dt to calculate churn
