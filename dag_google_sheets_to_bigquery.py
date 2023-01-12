"""demo"""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator

from functions import custom_failure_function
from functools import partial

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': partial(custom_failure_function,emails=['iliyana.tarpova@sparrks.de', 'mojtaba.peyrovi@sparrks.de'])
}

dag = DAG(
    'google_sheets_to_bigquery',
    default_args=default_args,
    description='google_sheets',
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=20))

t1 = SSHOperator(
    ssh_conn_id='ssh_default',
    task_id='trigger_code',
    command="python3 /home/mysql_to_bigquery_piplines/journey_analysis_to_bigquery.py",
    dag=dag)