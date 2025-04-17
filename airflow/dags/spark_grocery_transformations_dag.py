from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'spark_grocery_transformations',
    default_args=default_args,
    schedule_interval="@daily",  # or "@once" if manual
    catchup=False,
) as dag:

    spark_transform = SparkSubmitOperator(
        task_id='run_spark_transform',
        application='/opt/airflow/dags/sales_transformations.py',
        conn_id='spark_conn',
        packages='com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1',
        jars='https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar',
        conf={
            'spark.executor.memory': '4g',
            'spark.executor.cores': '2',
            'spark.driver.cores': '1',
            'spark.dynamicAllocation.enabled': 'false',
            'spark.hadoop.fs.gs.impl': 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem',
            'spark.hadoop.fs.gs.auth.service.account.enable': 'true',
            'spark.hadoop.google.cloud.auth.service.account.json.keyfile': '/credentials/google_credentials.json'
        },
        verbose=True,
        dag=dag
    )
