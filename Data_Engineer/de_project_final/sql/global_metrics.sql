CREATE TABLE stv2023121131__DWH.global_metrics (
	date_update TIMESTAMP(3) not null,
	currency_from NUMERIC(3) not null,
	amount_total FLOAT not null check(amount_total>=0),
	cnt_transactions int not null,
	avg_transactions_per_account NUMERIC(14, 5) not null,
	cnt_accounts_make_transactions int not null
)
ORDER BY date_update
SEGMENTED BY HASH(date_update::date) ALL NODES;