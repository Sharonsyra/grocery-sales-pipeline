import os

from pyspark.sql import SparkSession

BUCKET = os.environ.get("GCP_GCS_BUCKET")

spark = SparkSession.builder \
    .appName("GCS Transform") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .getOrCreate()
# .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/bitnami/spark/conf/gcs-keyfile.json") \
# .config("spark.jars",
#         "/opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.0.jar,/opt/spark/jars/gcs-connector-hadoop3-latest.jar") \

# GCS paths
input_path = f"gs://{BUCKET}/raw/categories.parquet"
output_path = f"gs://{BUCKET}/processed/categories"

# Load data
df = spark.read.option("header", True).parquet(input_path)

# Transform: simple filter or column change
df_transformed = df.withColumn("CatName", df.CategoryName)

# Save as Parquet
df_transformed.write.mode("overwrite").parquet(output_path)
