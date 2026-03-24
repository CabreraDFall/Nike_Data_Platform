from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os


PROJECT_ID = os.getenv("GCP_PROJECT_ID", "nike-data-platform")
BUCKET = os.getenv("GCP_GCS_BUCKET", "nike-data-lake")
DATASET = os.getenv("GCP_BQ_DATASET", "nike_dw")
LOCAL_DATA_PATH = "/opt/airflow/0_data"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='ingest_nike_to_gcp_v1',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['nike', 'gcp', 'cloud'],
) as dag:

    
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csvs_to_gcs',
        src=f'{LOCAL_DATA_PATH}/*.csv',
        dst='raw/',
        bucket=BUCKET,
    )

    
    load_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq_raw',
        bucket=BUCKET,
        source_objects=['raw/*.csv'],
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET}.raw_nike_data',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        
        gcp_conn_id='google_cloud_default',
    )

    
    dbt_run = BashOperator(
        task_id='dbt_run_cloud',
        bash_command='cd /opt/airflow/dbt && dbt run --target prod'
    )

    dbt_test = BashOperator(
        task_id='dbt_test_cloud',
        bash_command='cd /opt/airflow/dbt && dbt test --target prod'
    )

    upload_to_gcs >> load_to_bq >> dbt_run >> dbt_test
