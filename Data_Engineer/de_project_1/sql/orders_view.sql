create or replace view analysis.orders 
as
(select o.order_id, o.order_ts, o.user_id, o.bonus_payment, o.payment, o."cost", o.bonus_grant, osl.status_id as status
from
(select osl.order_id, max(osl.dttm) as maxdate from production.orderstatuslog osl group by osl.order_id order by osl.order_id) d
left join production.orderstatuslog osl on osl.order_id=d.order_id and osl.dttm=d.maxdate left join production.orders o on o.order_id=osl.order_id);