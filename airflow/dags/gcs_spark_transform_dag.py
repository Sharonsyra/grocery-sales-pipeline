from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "start_date": None,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

dag = DAG(
    dag_id="gcs_spark_transform",
    description="Reads from GCS, transforms via Spark, writes back to GCS",
    schedule_interval=None,
    default_args=default_args,
    catchup=False
)

start = DummyOperator(task_id="start", dag=dag)

spark_transform = SparkSubmitOperator(
    task_id='transform_gcs_data',
    application='/opt/airflow/dags/transform_gcs_to_gcs.py',
    conn_id='spark_conn',
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
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_transform >> end
