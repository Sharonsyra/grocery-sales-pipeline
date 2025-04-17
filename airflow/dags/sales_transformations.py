import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct, when, to_date, month, weekofyear, year, lit, avg, dayofweek, dayofmonth, quarter

spark = SparkSession.builder.appName("SalesETL").getOrCreate()

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET = "grocery_sales_pipeline"
    # os.environ.get("BIGQUERY_DATASET")
BUCKET = "grocery-sales-pipeline"
    # os.environ.get("GCP_GCS_BUCKET")
URL = f"{PROJECT_ID}.{BQ_DATASET}"

# Read external tables (BigQuery via GCS-backed Parquet or CSV)
sales = spark.read.format("bigquery").option("table", f"{URL}.sales_ext").option("useStorageApi", "false").load()
products = spark.read.format("bigquery").option("table", f"{URL}.products_ext").option("useStorageApi", "false").load()
customers = spark.read.format("bigquery").option("table", f"{URL}.customers_ext").option("useStorageApi", "false").load()
employees = spark.read.format("bigquery").option("table", f"{URL}.employees_ext").option("useStorageApi", "false").load()
cities = spark.read.format("bigquery").option("table", f"{URL}.cities_ext").option("useStorageApi", "false").load()
countries = spark.read.format("bigquery").option("table", f"{URL}.countries_ext").option("useStorageApi", "false").load()
categories = spark.read.format("bigquery").option("table", f"{URL}.categories_ext").option("useStorageApi", "false").load()

# Dim: Products
dim_product = products\
    .select("ProductID", "ProductName", "Price", "CategoryID", "Class", "ModifyDate", "Resistant", "IsAllergic",
            "VitalityDays") \
    .dropDuplicates()
dim_product.write.mode("overwrite").parquet(f"gs://{BUCKET}/dim_products")

# Dim: Customers
dim_customer = customers\
    .select("CustomerID", "FirstName", "MiddleInitial", "LastName", "CityID", "Address")\
    .dropDuplicates()
dim_customer.write.mode("overwrite").parquet(f"gs://{BUCKET}/dim_customers")

# Dim: Employees
dim_employee = employees\
    .select("EmployeeID", "FirstName", "MiddleInitial", "LastName", "BirthDate", "Gender", "CityID", "HireDate")\
    .dropDuplicates()
dim_employee.write.mode("overwrite").parquet(f"gs://{BUCKET}/dim_employee")

# Dim: Date
sales = sales\
    .select("SalesID", "SalesPersonID", "CustomerID", "ProductID", "Quantity", "Discount", "TotalPrice",
                        "SalesDate", "TransactionNumber")\
    .withColumn("SalesDate", to_date(col("SalesDate")))
dim_date = sales.select("SalesDate")\
    .dropDuplicates() \
    .withColumn("year", year("SalesDate")) \
    .withColumn("month", month("SalesDate")) \
    .withColumn("week", weekofyear("SalesDate")) \
    .withColumn("day_of_month", dayofmonth("SalesDate")) \
    .withColumn("day_of_week", dayofweek("SalesDate")) \
    .withColumn("quarter", quarter("SalesDate"))

dim_date.write.mode("overwrite").parquet(f"gs://{BUCKET}/dim_date")

dim_categories = categories\
    .select("CategoryID", "CategoryName")\
    .dropDuplicates()
dim_categories.write.mode("overwrite").parquet(f"gs://{BUCKET}/dim_categories")

dim_location = cities.join(countries, "CountryID", "left") \
    .select("CityID", "CityName", "Zipcode", "CountryID", "CountryName", "CountryCode") \
    .dropDuplicates()
dim_location.write.mode("overwrite").parquet(f"gs://{BUCKET}/dim_location")

# Fact Table
fact_sales = sales.join(products, "ProductID", "left") \
    .join(customers, "CustomerID", "left") \
    .join(employees, sales["SalesPersonID"] == employees["EmployeeID"], "left").select
fact_sales.write.partitionBy("SalesDate").mode("overwrite").parquet(f"gs://{BUCKET}/fact_sales")

# 1. Total Sales by Time
sales_by_time = fact_sales.groupBy("SalesDate").agg(sum("TotalPrice").alias("TotalSales"))
sales_by_time.write.mode("overwrite").parquet(f"gs://{BUCKET}/kpi/total_sales_by_date")

# 2. Top N Products by Revenue
top_products = fact_sales.groupBy("ProductID").agg(sum("TotalPrice").alias("Revenue")) \
    .orderBy(col("Revenue").desc())
top_products.write.mode("overwrite").parquet(f"gs://{BUCKET}/kpi/top_products")

# 3. Average Order Value
avg_order_value = fact_sales.groupBy("SalesID").agg(sum("TotalPrice").alias("order_total")) \
    .agg({"order_total": "avg"})
avg_order_value.write.mode("overwrite").parquet(f"gs://{BUCKET}/kpi/avg_order_value")

# 4. First purchase date per customer
first_purchase = fact_sales.groupBy("CustomerID").agg(min("SalesDate").alias("FirstPurchaseDate"))

# Join with fact to check repeat vs new
sales_with_customer_status = fact_sales.join(first_purchase, "CustomerID") \
    .withColumn("CustomerType", when(col("SalesDate") > col("FirstPurchaseDate"), lit("Repeat")).otherwise(lit("New")))

sales_with_customer_status.groupBy("SalesDate", "CustomerType").agg(countDistinct("CustomerID").alias("CustomerCount")) \
    .write.partitionBy("SalesDate").mode("overwrite").parquet(f"gs://{BUCKET}/kpi/customer_type_counts")

# 5. Price Trend by Category
price_trends = products.withColumn("month", month("ModifyDate")) \
    .groupBy("CategoryID", "month").agg(avg("Price").alias("avg_price"))
price_trends.write.mode("overwrite").parquet(f"gs://{BUCKET}/kpi/price_trends")

# 6. Sales by Employee
sales_by_employee = fact_sales.groupBy("EmployeeID").agg(sum("TotalPrice").alias("employee_sales"))
sales_by_employee.write.mode("overwrite").parquet(f"gs://{BUCKET}/kpi/sales_by_employee")

# 7. Revenue per employee per day
employee_daily_sales = fact_sales.groupBy("EmployeeID", "SalesDate") \
    .agg(sum("TotalPrice").alias("daily_revenue"))
employee_daily_sales.write.partitionBy("SalesDate").mode("overwrite").parquet(f"gs://{BUCKET}/kpi/employee_daily_sales")
