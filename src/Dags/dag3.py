from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

# Definir la ruta del script
script_path = os.path.join(os.path.dirname(__file__), 'main.py')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'nba_data_etl',
    default_args=default_args,
    description='DAG to extract NBA API data and load them into redshift database',
    schedule_interval='@daily',
)

# Definir la tarea que ejecutará el script
extract_and_load_data_task = PythonOperator(
    task_id='extract_and_load_data',
    python_callable=extract_and_load_data,
    dag=dag,
)

# dependencies
extract_and_load_data_task
