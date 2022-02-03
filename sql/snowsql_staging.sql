USE database KKBOX;

CREATE STAGE IF NOT EXISTS customer_churn;

PUT file:///path/to/file/transactions.csv @customer_churn;
PUT file:///path/to/file/transactions_v2.csv @customer_churn;
PUT file:///path/to/file/user_logs.csv @customer_churn;
PUT file:///path/to/file/user_logs_v2.csv @customer_churn;
PUT file:///path/to/file/members_v3.csv @customer_churn;

COPY INTO KKBOX.CHURN.TRANSACTIONS
FROM @customer_churn files = ('transactions.csv.gz', 'transactions_v2.csv.gz')
file_format = (type = CSV skip_header = 1);

COPY INTO KKBOX.CHURN.USER_LOGS
FROM @customer_churn files = ('user_logs.csv.gz', 'user_logs_v2.csv.gz')
file_format = (type = CSV skip_header = 1);

COPY INTO KKBOX.CHURN.MEMBERS
FROM @customer_churn files = ('members_v3.csv.gz')
file_format = (type = CSV skip_header = 1);

SELECT * FROM KKBOX.CHURN.TRANSACTIONS LIMIT 10;
SELECT * FROM KKBOX.CHURN.USER_LOGS LIMIT 10;
SELECT * FROM KKBOX.CHURN.MEMBERS LIMIT 10;

DROP STAGE IF EXISTS customer_churn;
