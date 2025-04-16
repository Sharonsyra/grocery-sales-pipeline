from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


###############################################
# Parameters
###############################################
spark_app_name = "Spark Hello World"

###############################################
# DAG Definition
###############################################

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": None,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

dag = DAG(
    dag_id="spark-test",
    description="This DAG runs a simple Pyspark app.",
    default_args=default_args
)

start = DummyOperator(task_id="start", dag=dag)

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/opt/airflow/dags/hello-world.py",
    name=spark_app_name,
    conn_id="spark_conn",
    verbose=1,
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end
