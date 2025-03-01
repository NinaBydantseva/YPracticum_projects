--0. Удаляем все создаваемые таблицы
drop table if exists shipping_country_rates cascade;
drop table if exists shipping_agreement cascade;
drop table if exists shipping_transfer cascade;
drop table if exists shipping_info cascade;
drop table if exists shipping_status cascade;

-- 1. Создаем пустую таблицу-справочник shipping_country_rates
--уникальные пары shipping_country и shipping_country_base_rate из таблицы shipping
drop table if exists shipping_country_rates cascade;
create table shipping_country_rates (
id serial,
shipping_country text,
shipping_country_base_rate numeric(14,3),
primary key(id)
);
create index id_index on shipping_country_rates(id);

--2. Создаем справочник тарифов доставки вендора по договору shipping_agreement
--источник данных - поле vendor_agreement_description ч/з ":"
drop table if exists shipping_agreement cascade;
create table shipping_agreement(
agreement_id bigint,
agreement_number varchar(10),
agreement_rate numeric(3,2),
agreement_comission numeric(3,2),
primary key(agreement_id)
);
create index agreement_id_index on shipping_agreement(agreement_id);

--3. Создаем справочник типов доставки shipping_transfer
--источник данных - поле shipping_transfer_description ч/з ":"
drop table if exists shipping_transfer cascade;
create table shipping_transfer(
shipping_transfer_id serial,
transfer_type varchar(4), --1p,3p
transfer_model varchar(10), --car,train,ship,airplain,multiple
shipping_transfer_rate numeric(14,3),
primary key(shipping_transfer_id)
);
create index shipping_transfer_id_index on shipping_transfer(shipping_transfer_id);

--4. Создаем справочник комиссии по странам shipping_info
-- 3 внешних ключа
drop table if exists shipping_info cascade;
create table shipping_info(
shipping_id bigint,
vendor_id bigint,
payment_amount numeric(14,2),
shipping_plan_datetime timestamp,
shipping_transfer_id bigint,
agreement_id bigint,
id bigint,
primary key(shipping_id),
foreign key (shipping_transfer_id) references shipping_transfer(shipping_transfer_id) on update cascade,
foreign key (id) references shipping_country_rates(id) on update cascade,
foreign key (agreement_id) references shipping_agreement(agreement_id) on update cascade
);
create index shipping_id_index on shipping_info(shipping_id);

--5. Создаем таблицу статусов доставки
-- для каждого уникального shipping_id отразить итоговое состояние;
--максимальные status и state по макс.state_datetime в shipping
drop table if exists shipping_status cascade;
create table shipping_status(
shipping_id bigint,
status text,
state text,
shipping_start_fact_datetime timestamp,
shipping_end_fact_datetime timestamp,
primary key(shipping_id)
);
create index shipping_status_id_index on shipping_status(shipping_id);

---------------------+++++++++++++++++++++---------------------------
--1а. Заполняем таблицу shipping_country_rates
create sequence shipping_country_rates_sequence start 1;
insert into shipping_country_rates
(id, shipping_country,shipping_country_base_rate)
select 
nextval ('shipping_country_rates_sequence') as id,
shipping_country,
shipping_country_base_rate
from
(select distinct
shipping_country,
shipping_country_base_rate
from shipping) as sh;
drop sequence shipping_country_rates_sequence;

--1б. Проверяем заполнение таблицы
-- select * from shipping_country_rates limit 10;

--2а. Заполняем таблицу shipping_agreement
insert into shipping_agreement 
(agreement_id, agreement_number, agreement_rate, agreement_comission)
select
   agreement_description[1]::bigint as agreement_id,
   agreement_description[2]::varchar(10) as agreement_number,
   agreement_description[3]::numeric(3,2) as agreement_rate,
   agreement_description[4]::numeric(3,2) as agreement_comission
   from (select distinct regexp_split_to_array(vendor_agreement_description,E'\\:+')
         as agreement_description from shipping s) as agreement_information; --order by agreement_id;
        
--2б. Проверяем, что таблица заполнена
-- select * from shipping_agreement limit 10;

--3а. Заполняем таблицу shipping_transfer
create sequence shipping_transfer_sequence start 1;
insert  into shipping_transfer
(shipping_transfer_id,transfer_type,transfer_model, shipping_transfer_rate)
select  
   nextval ('shipping_transfer_sequence') as shipping_transfer_id,
   transfer_description[1]::varchar(4) as transfer_type,
   transfer_description[2]::varchar(10) as transfer_model,
   shipping_transfer_rate
   from (select distinct regexp_split_to_array(shipping_transfer_description,E'\\:+') as transfer_description,
                shipping_transfer_rate
                from shipping s) as transfer_information;
drop sequence shipping_transfer_sequence; 

--3б. Проверяем, что таблица заполнена
-- select * from shipping_transfer limit 10;

--4а. Заполняем таблицу shipping_info
insert into shipping_info (shipping_id,vendor_id,payment_amount,
shipping_plan_datetime, shipping_transfer_id, agreement_id, id)
with
shippingtransfer as (select shipping_transfer_id, concat(transfer_type,':',transfer_model) as shipping_transfer_description, shipping_transfer_rate from shipping_transfer),
shippingagreement as (select agreement_id, concat_ws(':',agreement_id,agreement_number,agreement_rate::real ,agreement_comission::real) as agreement_description from shipping_agreement)
select distinct
s.shippingid as shipping_id, s.vendorid as vendor_id, s.payment_amount, s.shipping_plan_datetime,
shipping_transfer_id, sa.agreement_id, scr.id
from shipping s
join shippingtransfer st 
on s.shipping_transfer_rate=st.shipping_transfer_rate and s.shipping_transfer_description=st.shipping_transfer_description
join shipping_country_rates scr on s.shipping_country=scr.shipping_country and s.shipping_country_base_rate =scr.shipping_country_base_rate 
join shippingagreement sa on s.vendor_agreement_description=sa.agreement_description;

--4б. Проверяем, что таблица заполнена
-- select * from shipping_info limit 10;

--5а. Заполняем таблицу shipping_status
----5а.1 Создаем временную таблицу shipping_status_temp для размещения части информации для заполнения итоговой таблицы shipping_status
drop table if exists shipping_status_temp cascade;
create table shipping_status_temp(
shipping_id bigint,
shipping_start_fact_datetime timestamp,
shipping_end_fact_datetime timestamp,
primary key(shipping_id)
);
create index shipping_status_temp_id_index on shipping_status_temp(shipping_id);
----5а.2 Заполняем временную таблицу агрегированными данными по shipping_id, shipping_start_fact_datetime и shipping_end_fact_datetime
insert into shipping_status_temp
(shipping_id, shipping_start_fact_datetime, shipping_end_fact_datetime)
with
date_start as (select s2.shippingid, max(s2.state_datetime) as s_date from shipping s2 where s2.state='booked' group by s2.shippingid),
date_end as (select s3.shippingid, max(s3.state_datetime) as e_date from shipping s3 where s3.state='recieved' group by s3.shippingid)
select distinct s.shippingid as shipping_id, ds.s_date as shipping_start_fact_datetime, de.e_date as shipping_end_fact_datetime  
from shipping s   
left join date_start ds on ds.shippingid=s.shippingid
left join date_end de on de.shippingid=s.shippingid
order by s.shippingid;
----5а.3 Заполняем итоговую таблицу объединением таблиц shipping и shipping_status_temp
insert into shipping_status
(shipping_id, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
with
maxstatedaytime as (select s1.shippingid, max(s1.state_datetime) as maxdt from shipping s1 group by s1.shippingid)
select distinct
     s.shippingid,
     s.status,
     s.state,
     sst.shipping_start_fact_datetime,
     sst.shipping_end_fact_datetime
from shipping s
join maxstatedaytime md on md.shippingid=s.shippingid and md.maxdt=s.state_datetime
join shipping_status_temp sst on s.shippingid =sst.shipping_id; 

--5б. Проверяем, что таблица заполнена
-- select * from shipping_status limit 10;

--5в. Удаляем временную таблицу shipping_status_temp
drop table if exists shipping_status_temp cascade;

--6а. Создаем представление shipping_datamart
drop view if exists shipping_datamart;
create view shipping_datamart as (
select
      si.shipping_id,
      si.vendor_id,
      st.transfer_type,
      (case 
      	when ss.shipping_end_fact_datetime is not null then date_part('day',ss.shipping_end_fact_datetime-ss.shipping_start_fact_datetime)
      	else 0
      end) as full_day_at_shipping,
      (case 
      	when ss.shipping_end_fact_datetime>ss.shipping_start_fact_datetime then 1
      	else 0
      end) as is_delay,
      (case
      	when ss.status ='finished' then 1
      	else 0
      end) as is_shipping_finish,
      (case
      	when ss.shipping_end_fact_datetime>ss.shipping_start_fact_datetime then extract(day from age(ss.shipping_end_fact_datetime,ss.shipping_start_fact_datetime))
      	else 0
      end) as delay_day_at_shipping,
      si.payment_amount,  
      (scr.shipping_country_base_rate+sa.agreement_rate+st.shipping_transfer_rate)*si.payment_amount as vat,
      si.payment_amount*sa.agreement_comission as profit 
from shipping_info si
join shipping_transfer st on si.shipping_transfer_id=st.shipping_transfer_id
join shipping_status ss on si.shipping_id=ss.shipping_id
join shipping_country_rates scr on si.id =scr.id 
join shipping_agreement sa on si.agreement_id=sa.agreement_id);

--6б. Проверяем, что представление создано
-- select * from shipping_datamart;
