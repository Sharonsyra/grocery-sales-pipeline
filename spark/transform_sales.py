from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GrocerySalesTransform").getOrCreate()

sales_df = spark.read.parquet("/opt/spark-data/sales.parquet")
products_df = spark.read.parquet("/opt/spark-data/products.parquet")

# Join sales with product info
enriched_df = sales_df.join(products_df, on="product_id", how="left")

# Example: calculate total sales amount
enriched_df = enriched_df.withColumn("total_sales", enriched_df.quantity * enriched_df.price)

# Save result as new Parquet file
enriched_df.write.mode("overwrite").parquet("/opt/spark-data/enriched_sales.parquet")
