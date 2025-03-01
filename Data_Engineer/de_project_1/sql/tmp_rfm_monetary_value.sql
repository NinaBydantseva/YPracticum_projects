insert into analysis.tmp_rfm_monetary_value
(select user_id,
ntile(5) over (order by sum(case os."key" when 'Closed' then payment else 0 end) asc) as recency
from analysis.users u
left join analysis.orders o on u.id =o.user_id
left join analysis.orderstatuses os on o.status=os.id
where extract('year' from o.order_ts)>=2022
group by user_id
order by sum(case os."key" when 'Closed' then payment else 0 end) asc);