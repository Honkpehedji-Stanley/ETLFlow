"""Airflow DAG to orchestrate the ETL: extract -> transform -> load

This DAG uses BashOperator to run the scripts in the `scripts/` folder.
The project root is mounted into the Airflow image in docker-compose so paths work.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    description='Simple ETL pipeline: extract -> transform -> load',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract = BashOperator(
        task_id='extract',
        bash_command='python3 /opt/airflow/scripts/extract.py',
    )

    transform = BashOperator(
        task_id='transform',
        bash_command='python3 /opt/airflow/scripts/transform.py',
    )

    load = BashOperator(
        task_id='load',
        bash_command='python3 /opt/airflow/scripts/load.py',
    )

    extract >> transform >> load
