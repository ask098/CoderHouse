from airflow import DAG
from pathlib import Path
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import sys

#src_dir = "/opt/airflow/src"
#sys.path.insert(0, src_dir)

script_dir = "/usr/src/app"
sys.path.insert(0, script_dir)

#print("La ruta completa al directorio src es:", script_dir)
#print("La ruta sys path es:", sys.path)
                

from nba_etl import get_data, connect_database, create_table, insert_data, disconnect_database

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
    dag_id = 'nba_data_etl',
    default_args=default_args,
    description='DAG to extract NBA API data and load them into redshift database',
    schedule_interval='@daily',
    catchup=False
)

# tasks that execute script

get_data_task = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag,
)

connect_database_task = PythonOperator(
    task_id='connect_database',
    python_callable=connect_database,
    dag=dag
)

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag
)

insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag
)

disconnect_database_task = PythonOperator(
    task_id='disconnect_database',
    python_callable=disconnect_database,
    dag=dag
)

get_data_task >> connect_database_task >> create_table_task >> insert_data_task >> disconnect_database_task