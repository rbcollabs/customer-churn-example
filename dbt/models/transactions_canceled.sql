with t as (
  select
		A.msno,
		A.transaction_dt,
		max(A.ts) as ts,
		max(B.transaction_dt) as most_recent_transaction_dt
  from {{ ref('transactions_all') }} as A
  join {{ ref('transactions_all') }} as B
  where B.msno = A.msno
  and B.transaction_dt <= A.ts
	and B.transaction_dt > A.transaction_dt
  group by A.msno, A.transaction_dt
  order by A.msno, A.transaction_dt
)
select
	t.msno,
	t.transaction_dt,
	max(t.ts) as ts,
	max(t.most_recent_transaction_dt) as most_recent_transaction_dt,
	max(c.is_cancel) as most_recent_transaction_cancel
from t
join {{ ref('transactions_all') }} as C
where C.msno = t.msno
and C.transaction_dt = t.most_recent_transaction_dt
group by t.msno, t.transaction_dt
