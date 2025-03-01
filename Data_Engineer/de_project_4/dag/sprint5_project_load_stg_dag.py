import requests
import json
from psycopg2.extras import execute_values
import logging
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

task_logger = logging.getLogger('airflow.task')

# параметры подключения к ресурсам
postgres_conn = 'PG_WAREHOUSE_CONNECTION'
dwh_hook = PostgresHook(postgres_conn)

# параметры API
# задаем параметры для url couriers
#sort_field='_id'
#sort_direction='asc'
#limit=200

headers={'X-Nickname': 'NinaBydantseva','X-Cohort': '19','X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'}

# Создаем переменную для записи журнала AirFlow
task_logger = logging.getLogger('airflow.task')

# Создаем функцию загрузки данных о couriers, полученных через API
def upload_couriers(pg_schema, pg_table):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()
    # идемпотентность
    #dwh_hook.run(sql = f"DELETE FROM {pg_schema}.{pg_table}")

#сохраняем максимальный _id курьера для отсечения уже загруженных данных
    cursor.execute("select coalesce(max(_id),'0') from stg.couriers")
    row = cursor.fetchone()
    max_cour_id=row[0]    

    offset=0
    j=0
    while True:    
        url=f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers/?sort_field=_id&sort_direction=asc&offset={offset}"
        couriers_rep = requests.get(url, headers = headers).json()

        if len(couriers_rep) == 0:
            conn.commit()
            cursor.close()
            conn.close()
            task_logger.info(f'Writting {offset} rows')
            break

        max_cour_id_check=False
        columns = ','.join([i for i in couriers_rep[0]])
 
        if max_cour_id==str(0):
            m=-1
            max_cour_id_check=True                    
        else:
             for j in range(len(couriers_rep)):
                if str(couriers_rep[j].get('_id'))==str(max_cour_id):
                    max_cour_id_check=True
                    m=j                 

        if max_cour_id_check==True:
            values = [[value for value in couriers_rep[i].values()] for i in range(m+1,len(couriers_rep))]

            sql = f"INSERT INTO {pg_schema}.{pg_table} ({columns}) VALUES %s"
            execute_values(cursor, sql, values)

        offset += len(couriers_rep)  

# Создаем функцию загрузки данных о restaurants, полученных через API
def upload_restaurants(pg_schema, pg_table):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()
    # идемпотентность
    #dwh_hook.run(sql = f"DELETE FROM {pg_schema}.{pg_table}")

#сохраняем максимальный _id ресторана для отсечения уже загруженных данных
    cursor.execute("select coalesce(max(_id),'0') from stg.restaurants")
    row = cursor.fetchone()
    max_rest_id=row[0]

    offset=0
    j=0
    while True:    
        url=f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants/?sort_field=_id&sort_direction=asc&offset={offset}"
        restaurants_rep = requests.get(url, headers = headers).json()

        if len(restaurants_rep) == 0:
            conn.commit()
            cursor.close()
            conn.close()
            task_logger.info(f'Writting {offset} rows')
            break

        max_rest_id_check=False
        columns = ','.join([i for i in restaurants_rep[0]])      

        if max_rest_id==str(0):
            m=-1
            max_rest_id_check=True                  
        else:
             for j in range(len(restaurants_rep)):
                if str(restaurants_rep[j].get('_id'))==str(max_rest_id):
                    max_rest_id_check=True
                    m=j          

        if max_rest_id_check==True:
            values = [[value for value in restaurants_rep[i].values()] for i in range(m+1,len(restaurants_rep))]

            sql = f"INSERT INTO {pg_schema}.{pg_table} ({columns}) VALUES %s"
            execute_values(cursor, sql, values)

        offset += len(restaurants_rep)  

# Создаем функцию загрузки данных о deliveries, полученных через API
def upload_deliveries(pg_schema, pg_table):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()
    # идемпотентность
    #dwh_hook.run(sql = f"DELETE FROM {pg_schema}.{pg_table}")

#сохраняем максимальный order_id заказа для отсечения уже загруженных данных
    cursor.execute("select coalesce(max(order_id),'0') from stg.deliveries")
    row = cursor.fetchone()
    max_del_id=row[0]

    offset=0
    j=0
    while True:    
        url=f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries/?sort_field=_id&from=2024-01-20 00:00:00&sort_direction=asc&limit=50&offset={offset}"
        deliveries_rep = requests.get(url, headers = headers).json()

        if len(deliveries_rep) == 0:
            conn.commit()
            cursor.close()
            conn.close()
            task_logger.info(f'Writting {offset} rows')
            break

        max_del_id_check=False
        columns = ','.join([i for i in deliveries_rep[0]])

        if max_del_id==str(0):
            m=-1
            max_del_id_check=True                
        else:
             for j in range(len(deliveries_rep)):
                if str(deliveries_rep[j].get('_id'))==str(max_del_id):
                    max_del_id_check=True
                    m=j   
                    
        if max_del_id_check==True:
            values = [[value for value in deliveries_rep[i].values()] for i in range(m+1,len(deliveries_rep))]

            sql = f"INSERT INTO {pg_schema}.{pg_table} ({columns}) VALUES %s"
            execute_values(cursor, sql, values)

        offset += len(deliveries_rep)  

#Заполняем таблицу измерений restaurants:
def load_restaurants():

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("select max(restaurant_id) from dds.dm_restaurants")
    row = cursor.fetchone()
    row_dm=row[0]
    cursor.execute("select max(_id) from stg.restaurants")
    row = cursor.fetchone()
    row_stg=row[0]

    if not (row_dm==row_stg):
        cursor.execute("""
            with CTE as
            (select max(restaurant_id) as r_id from dds.dm_restaurants)        
            insert into dds.dm_restaurants (restaurant_id, restaurant_name)
            select 
            r."_id",
            r."name" 
            from stg.restaurants r, CTE
            where r."_id">CTE.r_id
                        ON CONFLICT ON CONSTRAINT dm_restaurants_restaurant_id_key 
                        DO NOTHING;
                """)
    
        conn.commit()

    cursor.close()
    conn.close()

    return 200

#Заполняем таблицу измерений couriers:
def load_couriers():

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("select max(courier_id) from dds.dm_couriers")
    row = cursor.fetchone()
    row_dm=row[0]
    cursor.execute("select max(_id) from stg.couriers")
    row = cursor.fetchone()
    row_stg=row[0]

    if not (row_dm==row_stg):
        cursor.execute("""
                with CTE as
                (select max(courier_id) as c_id from dds.dm_couriers)                     
                insert into dds.dm_couriers (courier_id, courier_name) 
                select
                c."_id" as courier_id,
                c."name" as courier_name
                from stg.couriers c, CTE
                where c."_id">CTE.c_id                      
                                ON CONFLICT ON CONSTRAINT dm_couriers_courier_id_key 
                                DO NOTHING;
                    """)
    
        conn.commit()

    cursor.close()
    conn.close()

    return 200

#Заполняем таблицу измерений orders:  
def load_orders():

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("select max(order_id) from dds.dm_orders")
    row = cursor.fetchone()
    row_dm=row[0]
    cursor.execute("select max(order_id) from stg.deliveries")
    row = cursor.fetchone()
    row_stg=row[0]

    if not (row_dm==row_stg):
        cursor.execute("""
            with CTE as
            (select distinct d.order_id, d.order_ts,d.sum, d.tip_sum  
                from stg.deliveries d), CTE_max as (select max(order_id) as o_id from dds.dm_orders)
            insert into dds.dm_orders (order_id, order_ts, orders_sum, tip_sum)
            select
            order_id,
            order_ts,
            "sum" as orders_sum,
            tip_sum
            from CTE, CTE_max
            where order_id>CTE_max.o_id
                            ON CONFLICT ON CONSTRAINT dm_orders_order_id_key
                            DO NOTHING;                   
                """)
    
        conn.commit()

    cursor.close()
    conn.close()

    return 200

#Заполняем таблицу измерений deliveries:    
def load_deliveries():

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("select max(delivery_id) from dds.dm_deliveries")
    row = cursor.fetchone()
    row_dm=row[0]
    cursor.execute("select max(delivery_id) from stg.deliveries")
    row = cursor.fetchone()
    row_stg=row[0]

    if not (row_dm==row_stg):
        cursor.execute("""
            with CTE as 
                    (select max(delivery_id) as d_id from dds.dm_deliveries)
            insert into dds.dm_deliveries (delivery_id, delivery_ts, address, rate)
            select distinct 
            d.delivery_id,
            d.delivery_ts,
            d.address,
            d.rate 
            from stg.deliveries d, CTE
            where d.delivery_id>CTE.d_id
                            ON CONFLICT ON CONSTRAINT dm_deliveries_delivery_id_key
                            DO NOTHING;                    
                """)
    
    conn.commit()

    cursor.close()
    conn.close()

    return 200    

#Заполняем таблицу фактов fct_courier_delivery:    
def load_fct_courier_delivery():

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("select max(order_id) from dds.dm_orders")
    row = cursor.fetchone()
    row_dm=row[0]

    cursor.execute("select max(do2.order_id) from dds.fct_courier_delivery fd join dds.dm_orders do2 on fd.order_id=do2.id")
    row = cursor.fetchone()
    row_fct=row[0]

    if not (row_dm==row_fct):

        cursor.execute("""
            with CTE as (select max(j2.order_id) as o_id from dds.fct_courier_delivery j1 join dds.dm_orders j2 on j1.order_id=j2.id)
            insert into dds.fct_courier_delivery (delivery_id, rate, courier_id, order_id, order_ts, orders_sum, tips_sum)
            select distinct 
            foo.del_id as delivery_id,
            foo.rate as rate,
            foo2.cur_id as courier_id,
            foo3.ord_id as order_id,
            foo3.order_ts as order_ts,
            foo3.orders_sum as orders_sum,
            foo3.tip_sum as tip_sum
            from CTE, stg.deliveries d 
            join (select dd.id as del_id, dd.delivery_id, dd.rate from dds.dm_deliveries dd) as foo on d.delivery_id=foo.delivery_id
            join (select dc.id as cur_id, dc.courier_id from dds.dm_couriers dc) as foo2 on d.courier_id=foo2.courier_id
            join (select do2.id as ord_id, do2.order_id, do2.order_ts, do2.orders_sum, do2.tip_sum from dds.dm_orders do2) as foo3  on d.order_id=foo3.order_id
            where d.order_id>CTE.o_id
            order by order_id asc;
                """)
    
    conn.commit()

    cursor.close()
    conn.close()

    return 200     

#заполняем витрину 
def load_cdm_m_courier_ledger():

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
    TRUNCATE TABLE cdm.m_courier_ledger CASCADE;
    DELETE FROM cdm.m_courier_ledger;

    insert into cdm.m_courier_ledger
    (courier_id,courier_name,settlement_year,settlement_month,orders_count,orders_total_sum,rate_avg,order_processing_fee,courier_order_sum,courier_tips_sum,courier_reward_sum)
    with CTE as(
    select
    	fd.courier_id,
	    dc.courier_name,
	    extract ("year" from (select now())) as settlement_year,	
	    extract ("month" from fd.order_ts) as settlement_month,	
	    avg(fd.rate) as rate_avg,
	    sum(fd.orders_sum) as orders_total_sum,
	    sum(fd.tips_sum) as courier_tips_sum,
	    count(fd.order_id) as orders_count
    from dds.fct_courier_delivery fd
    join dds.dm_couriers dc on fd.courier_id=dc.id 
    group by fd.courier_id, dc.courier_name, extract ("month" from fd.order_ts)
    ),
    CTE2 as (
    select
    	fd.courier_id,
    	sum(case
		    when rate_avg<4 and fd.orders_sum*0.05>=100 then fd.orders_sum*0.05
		    when rate_avg<4 and fd.orders_sum*0.05<100 then 100
		    when rate_avg>=4 and rate_avg<4.5 and fd.orders_sum*0.07>=150 then fd.orders_sum*0.07
		    when rate_avg>=4 and rate_avg<4.5 and fd.orders_sum*0.07<150 then 150		
		    when rate_avg>=4.5 and rate_avg<4.9 and fd.orders_sum*0.08>=175 then fd.orders_sum*0.08
		    when rate_avg>=4.5 and rate_avg<4.9 and fd.orders_sum*0.08<175 then 175	
		    when rate_avg>=4.9 and fd.orders_sum*0.1>=200 then fd.orders_sum*0.1
		    when rate_avg>=4.9 and fd.orders_sum*0.1<200 then 200			
	    end) as courier_order_sum
    from dds.fct_courier_delivery fd
    join CTE on fd.courier_id=cte.courier_id
    group by fd.courier_id)
    select distinct
    	fd.courier_id,
    	CTE.courier_name,
    	CTE.settlement_year,
	    CTE.settlement_month,
	    CTE.orders_count,
	    CTE.orders_total_sum, 
	    CTE.rate_avg,
    	CTE.orders_total_sum*0.25 as order_processing_fee,
	    CTE2.courier_order_sum,
    	CTE.courier_tips_sum,
	    (CTE2.courier_order_sum+CTE.courier_tips_sum*0.95) as courier_reward_sum
    from dds.fct_courier_delivery fd
    join CTE on fd.courier_id=cte.courier_id
    join CTE2 on fd.courier_id=cte2.courier_id
        """)
    
    conn.commit()

    cursor.close()
    conn.close()

    return 200 

# Оформляем DAG
default_args = {
    'owner':'airflow',
    'retries':1,
    'retry_delay': timedelta (seconds = 60)
}

dag = DAG('sprint5_project_load_stg_dag',
        start_date=datetime(2024, 1, 14),
        catchup=True,
        schedule_interval='@daily',
        max_active_runs=1,
        default_args=default_args)

t_upload_couriers = PythonOperator(
                task_id = 'upload_couriers',
                python_callable = upload_couriers,
                op_kwargs = {
                    'pg_schema' : 'stg',
                    'pg_table' : 'couriers'
                },
                dag = dag
    )

t_upload_restaurants = PythonOperator(
                task_id = 'upload_restaurants',
                python_callable = upload_restaurants,
                op_kwargs = {
                    'pg_schema' : 'stg',
                    'pg_table' : 'restaurants'
                },
                dag = dag
    )

t_upload_deliveries = PythonOperator(
                task_id = 'upload_deliveries',
                python_callable = upload_deliveries,
                op_kwargs = {
                    'pg_schema' : 'stg',
                    'pg_table' : 'deliveries'
                },
                dag = dag
    )

t_load_restaurants = PythonOperator(task_id='load_restaurants',
                                        python_callable=load_restaurants,
                                        dag=dag)

t_load_couriers = PythonOperator(task_id='load_couriers',
                                        python_callable=load_couriers,
                                        dag=dag)

t_load_orders = PythonOperator(task_id='load_orders',
                                        python_callable=load_orders,
                                        dag=dag)

t_load_deliveries = PythonOperator(task_id='load_deliveries',
                                        python_callable=load_deliveries,
                                        dag=dag)

t_load_fct_courier_delivery = PythonOperator(task_id='load_fct_courier_delivery',
                                        python_callable=load_fct_courier_delivery,
                                        dag=dag)

t_load_cdm_m_courier_ledger = PythonOperator(task_id='load_cdm_m_courier_ledger',
                                        python_callable=load_cdm_m_courier_ledger,
                                        dag=dag)

t_upload_couriers, t_upload_restaurants, t_upload_deliveries >> t_load_restaurants, t_load_couriers, t_load_orders, t_load_deliveries >> t_load_fct_courier_delivery >> t_load_cdm_m_courier_ledger