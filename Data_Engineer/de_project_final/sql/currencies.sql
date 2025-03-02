CREATE TABLE stv2023121131__STAGING.currencies (
	date_update TIMESTAMP(3) NULL,
	currency_code NUMERIC(3) NULL,
	currency_code_with NUMERIC(3) NULL,
	currency_with_div NUMERIC(5, 3) NULL
)
ORDER BY date_update
SEGMENTED BY HASH(date_update::date) ALL NODES;