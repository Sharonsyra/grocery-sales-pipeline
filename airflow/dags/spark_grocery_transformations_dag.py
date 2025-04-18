from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'start_date': None,
    'retries': 3
}

common_spark_config = {
    'application': '/opt/airflow/dags/sales_transformations.py',
    'conn_id': 'spark_conn',
    'packages': 'com.google.cloud.spark:spark-3.5-bigquery:0.42.1',
    'jars': 'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar',
    'conf': {
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
        'spark.executor.instances': '2',
        'spark.cores.max': '4',
        'spark.dynamicAllocation.enabled': 'false',
        'spark.hadoop.fs.gs.impl': 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem',
        'spark.hadoop.fs.gs.auth.service.account.enable': 'true',
        'spark.hadoop.google.cloud.auth.service.account.json.keyfile': '/credentials/google_credentials.json'
    },
    'verbose': True,
}

with DAG(
    'spark_grocery_transformations',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    dummy_job = SparkSubmitOperator(
        task_id='dummy_job_to_help_cache_bigquery',
        application_args=['--task', 'noop_step_1'],
        **common_spark_config
    )

    # 🔹 Dim Tables
    dim_products = SparkSubmitOperator(
        task_id='dim_products',
        application_args=['--task', 'dim_products'],
        **common_spark_config
    )

    dim_customers = SparkSubmitOperator(
        task_id='dim_customers',
        application_args=['--task', 'dim_customers'],
        **common_spark_config
    )

    dim_employees = SparkSubmitOperator(
        task_id='dim_employees',
        application_args=['--task', 'dim_employees'],
        **common_spark_config
    )

    dim_dates = SparkSubmitOperator(
        task_id='dim_dates',
        application_args=['--task', 'dim_dates'],
        **common_spark_config
    )

    dim_categories = SparkSubmitOperator(
        task_id='dim_categories',
        application_args=['--task', 'dim_categories'],
        **common_spark_config
    )

    dim_locations = SparkSubmitOperator(
        task_id='dim_locations',
        application_args=['--task', 'dim_locations'],
        **common_spark_config
    )

    # 🔹 Fact Table
    fact_sales = SparkSubmitOperator(
        task_id='fact_sales',
        application_args=['--task', 'fact_sales'],
        **common_spark_config
    )

    # 🔹 KPI Tables
    kpi_total_sales = SparkSubmitOperator(
        task_id='kpi_total_sales_by_date',
        application_args=['--task', 'kpi_total_sales_by_date'],
        **common_spark_config
    )

    kpi_top_products = SparkSubmitOperator(
        task_id='kpi_top_products',
        application_args=['--task', 'kpi_top_products'],
        **common_spark_config
    )

    kpi_avg_order_value = SparkSubmitOperator(
        task_id='kpi_avg_order_value',
        application_args=['--task', 'kpi_avg_order_value'],
        **common_spark_config
    )

    kpi_customer_type = SparkSubmitOperator(
        task_id='kpi_customer_type_counts',
        application_args=['--task', 'kpi_customer_type_counts'],
        **common_spark_config
    )

    kpi_price_trends = SparkSubmitOperator(
        task_id='kpi_price_trends',
        application_args=['--task', 'kpi_price_trends'],
        **common_spark_config
    )

    kpi_sales_by_employee = SparkSubmitOperator(
        task_id='kpi_sales_by_employee',
        application_args=['--task', 'kpi_sales_by_employee'],
        **common_spark_config
    )

    kpi_employee_daily_sales = SparkSubmitOperator(
        task_id='kpi_employee_daily_sales',
        application_args=['--task', 'kpi_employee_daily_sales'],
        **common_spark_config
    )

    dummy_job >> [dim_categories, dim_customers, dim_dates, dim_employees, dim_locations, dim_products] >> fact_sales
    fact_sales >> [
        kpi_total_sales,
        kpi_top_products,
        kpi_avg_order_value,
        kpi_customer_type,
        kpi_price_trends,
        kpi_sales_by_employee,
        kpi_employee_daily_sales,
    ]
