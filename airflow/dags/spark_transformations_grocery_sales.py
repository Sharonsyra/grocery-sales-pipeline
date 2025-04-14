from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    # "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id='spark_transformations_grocery_sales',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Run Spark transformations on grocery sales data',
    tags=['grocery-sales'],
) as dag:

    transform_task = SparkSubmitOperator(
        application="/opt/spark-apps/transform_sales.py",
        task_id="run_sales_transformations",
        verbose=True,
        conf={"spark.master": "spark://spark:7077"},
        application_args=["/opt/spark-data/sales.parquet"]
    )

    transform_task