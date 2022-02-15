begin;


set database_name = 'KKBOX';
set schema_name = 'CHURN';
set role_name = 'ACCOUNTADMIN';

-- create database & schema
create database if not exists identifier($database_name);

-- grant access
grant CREATE SCHEMA, MONITOR, USAGE
on database identifier($database_name)
to role identifier($role_name);

use database identifier($database_name);

create schema if not exists identifier($schema_name);

grant all privileges
on schema identifier($schema_name)
to role identifier($role_name);

create or replace TABLE MEMBERS (
	MSNO VARCHAR(16777216),
	CITY NUMBER(38,0),
	BD NUMBER(38,0),
	GENDER VARCHAR(16777216),
	REGISTERED_VIA NUMBER(38,0),
	REGISTRATION_INIT_TIME NUMBER(38,0)
);

create or replace TABLE TRANSACTIONS (
	MSNO VARCHAR(16777216),
	PAYMENT_METHOD_ID NUMBER(38,0),
	PAYMENT_PLAN_DAYS NUMBER(38,0),
	PLAN_LIST_PRICE NUMBER(38,0),
	ACTUAL_AMOUNT_PAID NUMBER(38,0),
	IS_AUTO_RENEW NUMBER(38,0),
	TRANSACTION_DATE NUMBER(38,0),
	MEMBERSHIP_EXPIRE_DATE NUMBER(38,0),
	IS_CANCEL NUMBER(38,0)
);

create or replace TABLE USER_LOGS (
	MSNO VARCHAR(16777216),
	DATE NUMBER(38,0),
	NUM_25 NUMBER(38,0),
	NUM_50 NUMBER(38,0),
	NUM_75 NUMBER(38,0),
	NUM_985 NUMBER(38,0),
	NUM_100 NUMBER(38,0),
	NUM_UNQ NUMBER(38,0),
	TOTAL_SECS FLOAT
);

commit;
