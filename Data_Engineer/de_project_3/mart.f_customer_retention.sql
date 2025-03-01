insert into mart.f_customer_retention(
   new_customers_count,
   returning_customers_count,
   refunded_customer_count,
   period_name,
   period_id,
   item_id,
   new_customers_revenue,
   returning_customers_revenue,
   customers_refunded
)
with customers as
         (select *
          from mart.f_sales
                   join mart.d_calendar on f_sales.date_id = d_calendar.date_id
          where week_of_year = DATE_PART('week', '{{ds}}'::DATE)),
     new_customers as
         (select customer_id from customers group by customer_id having count(*) = 1),
     returning_customers  as
         (select customer_id from customers group by customer_id having count(*) > 1),
     refunded_customers as
         (select customer_id from customers group by customer_id, status having status='refunded')
select
     new_customers.customers as new_customers_count,
     returning_customers.customers as returning_customers_count,
     refunded_customers.customers as refunded_customer_count,
     'week' as period_name,
     COALESCE(new_customers.week_of_year,
         returning_customers.week_of_year,
         refunded_customers.week_of_year) as period_id,
     customers.item_id as item_id,
     new_customers.revenue as new_customers_revenue,
     returning_customers.revenue as returning_customers_revenue,
     refunded_customers.refunded as customers_refunded
from customers
left join 
(select week_of_year,
             city_id,
             item_id,
             sum(payment_amount) as refunded,
             sum(quantity)       as items,
             count(*)            as customers
      from customers
      where status = 'refunded'
      and customer_id in (select customer_id from refunded_customers)
      group by week_of_year, city_id, item_id) as refunded_customers
      on customers.week_of_year=refunded_customers.week_of_year and customers.item_id=refunded_customers.item_id
left JOIN
(select week_of_year,
             city_id,
             item_id,
             sum(payment_amount) as revenue,
             sum(quantity)       as items,
             count(*)            as customers
      from customers
      where status = 'shipped'
      and customer_id in (select customer_id from new_customers)
      group by week_of_year, city_id, item_id) as new_customers
      on customers.week_of_year=new_customers.week_of_year and customers.item_id=new_customers.item_id
left JOIN
(select week_of_year,
             city_id,
             item_id,
             sum(payment_amount) as revenue,
             sum(quantity)       as items,
             count(*)            as customers
      from customers
      where status = 'shipped'
      and customer_id in (select customer_id from returning_customers)
      group by week_of_year, city_id, item_id) as returning_customers   
      on customers.week_of_year=returning_customers.week_of_year and customers.item_id=returning_customers.item_id;