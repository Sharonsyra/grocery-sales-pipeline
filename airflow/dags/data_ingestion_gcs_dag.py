import os

import pyarrow.csv as pv
import pyarrow.parquet as pq
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from pathlib import Path

from airflow import DAG

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "grocery-sales-dataset"
dataset_url = f"https://www.kaggle.com/api/v1/datasets/download/andrexibiza/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'grocery_sales_pipeline')


def format_to_parquet(src_directory):
    for filename in os.listdir(src_directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(src_directory, filename)
            print(f"Processing: {filename}")

            table = pv.read_csv(file_path)  # just pass the path, not a file object
            parquet_path = file_path.replace('.csv', '.parquet')

            pq.write_table(table, parquet_path)
            print(f"Written: {parquet_path}")


def upload_to_gcs(bucket, object_directory, local_directory):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_directory: target path & file-name
    :param local_directory: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    for file_path in Path(local_directory).glob("*.parquet"):
        object_name = f"{object_directory}/{file_path.name}"

        blob = bucket.blob(object_name)
        blob.upload_from_filename(f"{local_directory}/{file_path.name}")


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
        dag_id="data_ingestion_gcs_dag",
        schedule_interval="@daily",
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        tags=['grocery-sales'],
) as dag:
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}.zip"
    )

    unzip_dataset_task = BashOperator(
        task_id='unzip_dataset_task',
        bash_command=f"unzip {path_to_local_home}/{dataset_file}.zip -d {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_directory": f"{path_to_local_home}/{dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_directory": "raw",
            "local_directory": f"{path_to_local_home}/{dataset_file}/",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/*.parquet"],
            },
        },
    )

    download_dataset_task >> unzip_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task
