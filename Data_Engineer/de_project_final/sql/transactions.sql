CREATE TABLE stv2023121131__STAGING.transactions (
	operation_id uuid NULL,
	account_number_from int NULL,
	account_number_to int NULL,
	currency_code NUMERIC(3) NULL,
	country VARCHAR(30) NULL,
	status VARCHAR(30) NULL,
	transaction_type VARCHAR(30) NULL,
	amount int NULL,
	transaction_dt TIMESTAMP(3) NULL
)
ORDER BY operation_id, transaction_dt
SEGMENTED BY HASH(operation_id, transaction_dt::date) ALL NODES;