from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import boto3
import pendulum

AWS_ACCESS_KEY_ID = "xxx"
AWS_SECRET_ACCESS_KEY = "xxx"

def fetch_s3_file(bucket: str, key: str) -> str:
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )

bash_command_tmpl = """
echo {{ params.files }}
"""

@dag(schedule_interval=None, start_date=pendulum.parse('2024-01-01'))
def project6_dag_get_data():
    key = 'group_log.csv'
    fetch_tasks = PythonOperator(
            task_id=f'fetch_{key}',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': key},
    )
    
    print_10_lines_of_file = BashOperator(
        task_id='print_10_lines_of_file',
        bash_command=bash_command_tmpl,
        params={'files': [f'/data/{key}']}
    )

    fetch_tasks >> print_10_lines_of_file

_ = project6_dag_get_data()

#df_group_log['user_id_from'] = pd.array(df_group_log['user_id_from'], dtype="Int64").