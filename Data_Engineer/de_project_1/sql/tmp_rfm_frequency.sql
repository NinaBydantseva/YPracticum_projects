insert into analysis.tmp_rfm_frequency
(select user_id,
ntile(5) over (order by count(case os."key" when 'Closed' then 1 else null end) asc) as frequency
from analysis.users u
left join analysis.orders o on u.id =o.user_id
left join analysis.orderstatuses os on o.status=os.id
where extract('year' from o.order_ts)>=2022
group by user_id
order by count(case os."key" when 'Closed' then 1 else null end) asc);
