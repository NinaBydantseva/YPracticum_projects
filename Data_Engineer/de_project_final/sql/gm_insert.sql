WITH CTE as (select * from stv2023121131__STAGING.currencies c1 where c1.currency_code_with=420 and c1.date_update::date=(current_date-1)::date
			union select (current_date-1)::timestamp,420::integer,420::integer,1)
SELECT
	(current_date-1)::timestamp as date_update,
	t.currency_code as currency_from,
	sum(abs(t.amount)*case when t.currency_code = 420 then 1 else c.currency_with_div end) as amount_total,
	count(t.account_number_from) as cnt_transactions,
	count(t.account_number_from)/count(distinct(t.account_number_from)) as avg_transactions_per_account,
	count(distinct(t.account_number_from)) as cnt_accounts_make_transactions
FROM stv2023121131__STAGING.transactions t join CTE c on t.currency_code=c.currency_code 
WHERE t.status='done'
	  and t.account_number_from>=0
 	  and date_trunc('day', transaction_dt)::date=(current_date-1)::date
      and t.transaction_type in ('c2a_incoming',
                                 'c2b_partner_incoming',
                                 'sbp_incoming',
                                 'sbp_outgoing',
                                 'transfer_incoming',
                                 'transfer_outgoing')
group BY t.currency_code;