import datetime as dt
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 'airflow',
                                'start_date':datetime(2024, 2, 24),
                                }

dag_spark = DAG(
                        dag_id = "7PROJECT_3STEPS",
                        default_args=default_args,
                        schedule_interval=None,
                        )

# объявляем задачу с помощью SparkSubmitOperator
spark_submit_users_home_travel = SparkSubmitOperator(
                        task_id='spark_submit_users_home_travel_task',
                        dag=dag_spark,
                        application ='/lessons/users_home_travel.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ["/user/master/data/geo/events","2022-02-03","part-01578-82f846d5-74a4-44e0-8fa9-de643f825932.c000.snappy.parquet","/user/nybydantse/analytics/users_home_travel", "27"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 4,
                        executor_memory = '4g'
                        )

# объявляем задачу с помощью SparkSubmitOperator
spark_submit_cities_events_by_range = SparkSubmitOperator(
                        task_id='spark_submit_cities_events_by_range_task',
                        dag=dag_spark,
                        application ='/lessons/cities_events_by_range.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ["/user/master/data/geo/events","2022-02-03","part-01719-82f846d5-74a4-44e0-8fa9-de643f825932.c000.snappy.parquet","/user/nybydantse/analytics/cities_events_by_range"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

# объявляем задачу с помощью SparkSubmitOperator
spark_submit_users_friends_recomendation = SparkSubmitOperator(
                        task_id='spark_submit_users_friends_recomendation_task',
                        dag=dag_spark,
                        application ='/lessons/users_friends_recomendation.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ["/user/master/data/geo/events","2022-02-03","part-01719-82f846d5-74a4-44e0-8fa9-de643f825932.c000.snappy.parquet","/user/nybydantse/analytics/users_friends_recomendation"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

[spark_submit_users_home_travel,spark_submit_cities_events_by_range,spark_submit_users_friends_recomendation]