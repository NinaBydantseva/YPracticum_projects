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
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.decorators import dag

import pandas as pd
import pendulum
import vertica_python

# параметры подключения к Vertica
vertica_connection = 'VERTICA_CONNECTION'
#dwh_hook = VerticaHook(vertica_conn)

# параметры подключения к PostGre
postgres_connection = 'PG_WAREHOUSE_CONNECTION'
#stg_hook = PostgresHook(postgres_conn)

#set postgresql connectionfrom basehook
psql_conn = BaseHook.get_connection(postgres_connection)
vrtk_conn = BaseHook.get_connection(vertica_connection)

##init test connection
conn = psycopg2.connect(f"dbname='{psql_conn.schema}' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
cur = conn.cursor()
cur.close()
conn.close()

def date_array(date_start: datetime, date_end: datetime) -> List[datetime]:
    mydates = pd.date_range(date_start, date_end).tolist()
    return mydates

def get_transactions(date_update: datetime) -> List[Dict]:
    conn = psycopg2.connect(f"dbname='{psql_conn.schema}' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()        
    # Формируем выборку из PG по filter             
    cur.execute(f"""SELECT
                            operation_id,
                            account_number_from,
                            account_number_to,
                            currency_code,
                            country,
                            status,
                            transaction_type,
                            amount,
                            transaction_dt
                        FROM public.transactions
                        WHERE transaction_dt::date='{date_update}';
                    """)     
    return cur.fetchall()      

def get_currencies(date_update: datetime) -> List[Dict]:
    conn = psycopg2.connect(f"dbname='{psql_conn.schema}' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()         
    # Формируем выборку из PG по filter            
    cur.execute(f"""SELECT
                            date_update,
                            currency_code,
                            currency_code_with,
                            currency_with_div
                        FROM public.currencies
                        WHERE date_update::date='{date_update}';
                    """)             
    return cur.fetchall()

# Загружаем данные в Vertica
def load_dataset_from_ps_to_vertica(
    date_update: datetime,   
    dataset_name: str,
    schema: str,
    table: str,
    cols: List[str],
    type_override: Optional[Dict[str, str]] = None,
):

    if dataset_name=='get_transactions':
        df = pd.DataFrame(data=get_transactions(date_update),columns=cols)
    elif dataset_name=='get_currencies':
        df = pd.DataFrame(data=get_currencies(date_update),columns=cols)
    num_rows = len(df)
    vertica_conn = vertica_python.connect(
                        host=vrtk_conn.host,
                        port=vrtk_conn.port,
                        user=vrtk_conn.login,
                        password = vrtk_conn.password)
    columns = ', '.join(cols)
    copy_expr = f"""
                        COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '"'
                    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1

    vertica_conn.close()


@dag(schedule="@daily", start_date=pendulum.parse('2022-10-01'), catchup=False)
def prj_final_dag_load_STG():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    today=date.today()
    ddate=date_array(today,today)
    for task in range(ddate.__len__()):    
        load_transactions = PythonOperator(
            task_id=str('load_transactions_'+str(task)),
            python_callable=load_dataset_from_ps_to_vertica,
            op_kwargs={
                'date_update': str(ddate[task]),
                'dataset_name': 'get_transactions',
                'schema': 'STV2023121131__STAGING',
                'table': 'transactions',
                'cols': ['operation_id','account_number_from','account_number_to','currency_code','country','status','transaction_type','amount','transaction_dt']
            },
        )

        load_currencies = PythonOperator(
            task_id=str('load_currencies_'+str(task)),
            python_callable=load_dataset_from_ps_to_vertica,
            op_kwargs={
                'date_update': str(ddate[task]),
                'dataset_name': 'get_currencies',
                'schema': 'STV2023121131__STAGING',
                'table': 'currencies',
                'cols': ['date_update','currency_code','currency_code_with','currency_with_div']
            },
        )

    start >> [load_transactions, load_currencies] >> end


_ = prj_final_dag_load_STG()