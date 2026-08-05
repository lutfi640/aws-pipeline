from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'imam',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'world_bank_bronze_pipeline',
    default_args=default_args,
    description='Pipeline World Bank - Extract API to Bronze S3',
    schedule=None, # Manual trigger dulu buat testing
    catchup=False,
    tags=['aws', 'world_bank', 'bronze']
) as dag:

    # -------------------------------------------------------------
    # TASK 1: Extract API to Bronze (Narik 3 JSON sekaligus)
    # -------------------------------------------------------------
    extract_task = BashOperator(
        task_id='extract_wb_api_to_bronze',
        # Pastikan file python ini udah ada di dalam folder EC2/Docker lo
        bash_command='python /opt/airflow/scripts/extractors/extract_worldBank.py',
    )

    # Pipeline Flow (Cuma 1 task aja)
    extract_task