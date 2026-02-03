from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('sleep_etl',
         start_date=datetime(2025,1,1),
         schedule_interval=None,
         default_args=default_args,
         catchup=False,
         tags=['etl']) as dag:

    extract = BashOperator(
        task_id='extract_load_raw',
        bash_command='python /opt/airflow/scripts/extract_load_raw.py'
    )

    transform_load = BashOperator(
        task_id='transform_load',
        bash_command='python /opt/airflow/scripts/transform_load.py'
    )

    extract >> transform_load
