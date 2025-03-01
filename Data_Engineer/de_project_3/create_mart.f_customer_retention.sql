--создаем таблицу-витрину mart.f_customer_retention
drop table f_customer_retention if exists cascade;
create table mart.f_customer_retention (
   id SERIAL,
   new_customers_count bigint,
   returning_customers_count bigint,
   refunded_customer_count bigint,
   period_name varchar(100),
   period_id int,
   item_id int4,
   new_customers_revenue numeric(14,2),
   returning_customers_revenue numeric(14,2),
   customers_refunded bigint,
   PRIMARY KEY 		(id),
   FOREIGN KEY 		(item_id) REFERENCES mart.d_item (item_id) ON UPDATE cascade
);
comment on column mart.f_customer_retention.new_customers_count is 'Кол-во новых клиентов (тех, которые сделали только один заказ за рассматриваемый промежуток времени)';
comment on column mart.f_customer_retention.returning_customers_count is 'Кол-во вернувшихся клиентов (тех, которые сделали только несколько заказов за рассматриваемый промежуток времени)';
comment on column mart.f_customer_retention.refunded_customer_count is 'Кол-во клиентов, оформивших возврат за рассматриваемый промежуток времени'; 
comment on column mart.f_customer_retention.period_name is 'Weekly';
comment on column mart.f_customer_retention.period_id is 'Идентификатор периода (номер недели или номер месяца)';
comment on column mart.f_customer_retention.item_id is 'Идентификатор категории товара';
comment on column mart.f_customer_retention.new_customers_revenue is 'Доход с новых клиентов';
comment on column mart.f_customer_retention.returning_customers_revenue is 'Доход с вернувшихся клиентов';
comment on column mart.f_customer_retention.customers_refunded is 'Количество возвратов клиентов';