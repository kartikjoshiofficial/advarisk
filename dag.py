from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from psycopg2 import psycopg2

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2023,05,8),
    'email' : ['airflow@example.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

dag =DAG(
    'company_sales_dag',
    default_args=default_args,
    description='Company Sales'
)

t1 = BashOperator(
    task_id='compant_sales_etl',
    bash_command='python D:\Data Engineer Projects\advarisk\etl.py',
    dag=dag)

t1
