from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt

default_args = {
    'owner': 'DataCo',
    'start_date': dt.datetime(2026, 3, 1),
    'retries': 1
}

with DAG('dataco_etl',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         ) as dag:

    extract = BashOperator(task_id='extract', bash_command='sudo -u airflow python /opt/airflow/scripts/extract.py')
    transform = BashOperator(task_id='transform', bash_command='sudo -u airflow python /opt/airflow/scripts/transform.py')
    load = BashOperator(task_id='load', bash_command='sudo -u airflow python /opt/airflow/scripts/load.py')
    
    
extract >> transform >> load