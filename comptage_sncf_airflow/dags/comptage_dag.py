from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['paulcoffi2002@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)}

@dag(
    dag_id = "comptage_dag",
    start_date = datetime(2023, 1, 1),
    schedule = "@daily",
    catchup= False,
    default_args = default_args
)
def comptage_dag():

    # Task 1: Download data from the API and save it to S3

    download_data = HttpToS3Operator(
        task_id='download_data',
        http_conn_id='sncf_comptage_endpoint',
        endpoint='/api/explore/v2.1/catalog/datasets/comptage-voyageurs-trains-transilien/exports/json',
        method='GET',
        s3_bucket='comptagesncf',
        aws_conn_id='cloudfare_conn',
        s3_key='data_{{ds}}.json',
        replace=True
    )

    # Task 2: Run ingestion job from databricks to load data into the lakehouse

    run_ingestion = DatabricksRunNowOperator(
        task_id='run_ingestion',
        databricks_conn_id='databricks_default',
        job_id={{var.value.databricks_job_id}}
    )

    # Task 3: Run dbt staging models and tests to transform the data

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/airflow/dags/comptage_sncf_airflow && dbt run --staging && dbt test --staging'
    )

    # Task 4: Run dbt mart models and tests to transform the data
    run_dbt_mart = BashOperator(
        task_id='run_dbt_mart',
        bash_command='cd /opt/airflow/dags/comptage_sncf_airflow && dbt run --mart && dbt test --mart'
    )

    download_data >> run_ingestion >> run_dbt >> run_dbt_mart

comptage_dag()

