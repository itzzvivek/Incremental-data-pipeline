from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

SCRIPT = "/app/scripts/run_pipeline.py"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='coinapi_incremental',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['coinapi', 'incremental'],
) as dags:
    
    def trigger_pipeline():
        os.system(f"python {SCRIPT} --once")

    run_task = PythonOperator(
        task_id='run_incremental_pipeline',
        python_callable=trigger_pipeline
    )

    run_task