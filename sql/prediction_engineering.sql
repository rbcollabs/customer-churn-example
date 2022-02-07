begin;

create or replace view as kkbox.churn.churn_model_definition(
	with t as (
		select
			msno,
			ts,
			lag(transaction_dt) over (partition by msno order by transaction_dt desc) as next_transaction,
			first_value(transaction_dt) over (partition by msno order by transaction_dt desc) as most_recent_transaction,
			datediff(days,transaction_dt,expiration_dt) as days_since_last_transaction,
			datediff(days,expiration_dt,date('2017-04-01')) as days_expired, --second date here is just "today's date". For this problem, the most recent month is 4/2017
			case
			       when (next_transaction is NULL and days_expired>30) then True --no next transaction, if you are more than 30 days expired then you are churn
			       when (next_transaction is not Null and datediff(days,expiration_dt,date(next_transaction,'YYYY-MM-DD'))>30) then True --there is a next transaction, but you waited more than 30 days to renew, so this is churn
			       else False
			end as is_churn,
			--case
			--       when (is_churn=True and next_transaction is not NULL and most_recent_transaction > expiration_dt) then True --there was churn, but then another transaction after the expiration date.
			--       when (is_churn=False) then NULL --no churn, so not relevant
			--       else False --churn and no return
			--end as churn_and_return,
			case
				when ts < '2017-01-01' and ts >= '2016-06-01' then 'TRAIN'
				when ts < '2017-02-01' and ts >= '2017-01-01' then 'VALI'
				when ts < '2017-03-01' and ts >= '2017-02-01' then 'TEST'
				else 'PREDICT_ME'
			end as SPLIT,
		datediff(days,(select max(log_date) from kkbox.churn.user_logs_all where msno=A.msno and log_date<=A.ts),ts) as days_since_last_log
		from kkbox.churn.transactions_final as A
	)
	select * from t
	where ts >= '2016-06-01'
	and ts <= '2017-04-01'
	--remove the next two lines if you don't want to filter on only users who have log data
	and msno in (select distinct msno from kkbox.churn.user_logs_all)
	and ts in (select distinct ts from kkbox.churn.user_logs_all where msno = t.msno)
);

commit;
