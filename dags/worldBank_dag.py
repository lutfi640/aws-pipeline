from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator as LambdaInvokeOperator
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'imam',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'world_bank_pipeline_v1',
    default_args=default_args,
    description='Pipeline World Bank - Bronze Extraction & Silver Dimension/Fact Transformer',
    schedule=None,  # Manual trigger via Airflow UI untuk testing
    catchup=False,
    tags=['aws', 'world_bank', 'bronze', 'silver']
) as dag:

    today_str = datetime.now().strftime('%Y-%m-%d')

    # =============================================================
    # TASK 1: EXTRACT API TO BRONZE (JSON S3)
    # =============================================================
    extract_task = BashOperator(
        task_id='extract_wb_api_to_bronze',
        bash_command='python /opt/airflow/scripts/extractors/extract_world_bank.py',
    )

    # =============================================================
    # TASK 2: GENERATE DIMENSIONS (dim_country & dim_indicator)
    # =============================================================
    try:
        with open('/opt/airflow/lambda/transformers/dim_worldBank.py', 'r') as file:
            dim_code_string = file.read()
    except Exception as e:
        dim_code_string = f"print('Gagal membaca file script lokal: {str(e)}')"
        raise e

    generate_dim = LambdaInvokeOperator(
        task_id='generate_dim_world_bank',
        function_name='earthquake-transformer-docker', # Container Lambda Docker yang sama
        payload=json.dumps({
            "code": dim_code_string, 
            "bucket": "learn-aws-imam", 
            "date": today_str
        }),
        aws_conn_id='aws_default',
        log_type='Tail'
    )

    # =============================================================
    # TASK 3: TRANSFORM SILVER FACT (fact_indicator Parquet)
    # =============================================================
    # try:
    #     with open('/opt/airflow/lambda/transformers/transform_world_bank.py', 'r') as file:
    #         transform_code_string = file.read()
    # except Exception as e:
    #     transform_code_string = f"print('Gagal membaca file script lokal: {str(e)}')"
    #     raise e

    # transform_fact = LambdaInvokeOperator(
    #     task_id='transform_wb_bronze_to_silver',
    #     function_name='earthquake-transformer-docker',
    #     payload=json.dumps({
    #         "code": transform_code_string, 
    #         "bucket": "learn-aws-imam", 
    #         "date": today_str
    #     }),
    #     aws_conn_id='aws_default',
    #     log_type='Tail'
    # )

    # =============================================================
    # PIPELINE DEPENDENCY FLOW
    # =============================================================
    extract_task >> generate_dim #>> transform_fact