import contextlib
import hashlib
import json
from typing import Dict, List, Optional
from datetime import datetime, date

import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.decorators import dag

import pandas as pd
import pendulum
import vertica_python
import io

# параметры подключения к Vertica
vertica_connection = 'VERTICA_CONNECTION'
#dwh_hook = VerticaHook(vertica_conn)

# параметры подключения к PostGre
postgres_connection = 'PG_WAREHOUSE_CONNECTION'
#stg_hook = PostgresHook(postgres_conn)

#set postgresql connectionfrom basehook
psql_conn = BaseHook.get_connection(postgres_connection)
vrtk_conn = BaseHook.get_connection(vertica_connection)

def date_array(date_start: datetime, date_end: datetime) -> List[datetime]:
    mydates = pd.date_range(date_start, date_end).tolist()
    return mydates

def get_metrics(yesterday: datetime, conn: vertica_python.connect) -> List[Dict]:

    cur = conn.cursor()       
    d = datetime.strptime(yesterday, "%Y-%m-%d %H:%M:%S")
    d = d.date()  
    yesterday=d.isoformat()
    # Формируем выборку из Vertica по filter            
    cur.execute(f"""
                    with CTE as (select * from stv2023121131__STAGING.currencies c1 where c1.currency_code_with=420 and c1.date_update::date='{yesterday}'
			        union select to_timestamp('{yesterday}','YYYY-MM-DD HH24:MI:SS.SSS'),420::integer,420::integer,1)
                    select
                        to_timestamp('{yesterday}','YYYY-MM-DD HH24:MI:SS.SSS')::date as date_update,
                        t.currency_code as currency_from,
                        sum(abs(t.amount)*case when t.currency_code = 420 then 1 else c.currency_with_div end) as amount_total,
                        count(t.account_number_from) as cnt_transactions,
                        count(t.account_number_from)/count(distinct(t.account_number_from)) as avg_transactions_per_account,
                        count(distinct(t.account_number_from)) as cnt_accounts_make_transactions
                    from stv2023121131__STAGING.transactions t join CTE c on t.currency_code=c.currency_code 
                    where t.status='done' and t.account_number_from>=0 and t.account_number_to>=0 
                        and date_trunc('day', transaction_dt)::date='{yesterday}'
                        and t.transaction_type in ('c2a_incoming',
                                                    'c2b_partner_incoming',
                                                    'sbp_incoming',
                                                    'sbp_outgoing',
                                                    'transfer_incoming',
                                                    'transfer_outgoing')
                    group by t.currency_code;
                """)  

    return cur.fetchall()

# Загружаем данные в Vertica
def load_dataset_to_mart(
    yesterday: datetime,   
    schema: str,
    table: str,
    cols: List[str],
    type_override: Optional[Dict[str, str]] = None,
):

        vertica_conn = vertica_python.connect(
                            host=vrtk_conn.host,
                            port=vrtk_conn.port,
                            user=vrtk_conn.login,
                            password = vrtk_conn.password)
        columns = ', '.join(cols)
        cur = vertica_conn.cursor() 
        #формируем датафрейм запросом к таблицам в Vertica
        df = pd.DataFrame(data=get_metrics(yesterday, vertica_conn),columns=cols)
        #конвертируем датафрейм в список
        lists = df.values.tolist()
        # формируем запрос на вставку данных
        insert_qry = f" INSERT INTO {schema}.{table} ({columns}) VALUES (%s,%s,%s,%s,%s,%s)"
        #загружаем данные списка в БД
        for i in range(len(lists)):
            cur.execute(insert_qry, lists[i])
        vertica_conn.commit()
        vertica_conn.close()


@dag(schedule="@daily", start_date=pendulum.parse('2022-10-01'), catchup=False)
def prj_final_dag_load_CDM():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    yesterday=date.today() - timedelta(days=1)
    ddate=date_array('2022-10-01','2022-10-31')
    for task in range(ddate.__len__()):
        load_global_metrics = PythonOperator(
            task_id=str(task),
            python_callable=load_dataset_to_mart,
            op_kwargs={
                'yesterday': str(ddate[task]),
                'schema': 'stv2023121131__DWH',
                'table': 'global_metrics',
                'cols': ['date_update','currency_from','amount_total','cnt_transactions','avg_transactions_per_account','cnt_accounts_make_transactions']
            },
        )

        start >> load_global_metrics >> end


_ = prj_final_dag_load_CDM()