insert into analysis.tmp_rfm_recency
(select user_id,
ntile(5) over (order by max(case os."key" when 'Closed' then o.order_ts else '2022-01-01' end) asc) as recency
from analysis.users u
left join analysis.orders o on u.id =o.user_id
left join analysis.orderstatuses os on o.status=os.id
where extract('year' from o.order_ts)>=2022
group by user_id
order by max(case os."key" when 'Closed' then o.order_ts else '2022-01-01' end) asc);
