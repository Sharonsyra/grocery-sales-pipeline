import argparse
import os

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, sum, countDistinct, when, to_date, month, weekofyear, year, lit, avg, dayofweek, \
    dayofmonth, from_unixtime, quarter

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET = os.environ.get("BIGQUERY_DATASET")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
URL = f"{PROJECT_ID}.{BQ_DATASET}"

source_path = f"gs://{BUCKET}/raw"
temp_path = f"gs://{BUCKET}/temp"


def create_spark_session():
    return SparkSession.builder.appName("GroceryDataTransformations").getOrCreate()


def sales_df(spark):
    sales_schema = types.StructType([
        types.StructField('SalesID', types.LongType(), True),
        types.StructField('SalesPersonID', types.LongType(), True),
        types.StructField('CustomerID', types.LongType(), True),
        types.StructField('ProductID', types.LongType(), True),
        types.StructField('Quantity', types.LongType(), True),
        types.StructField('Discount', types.DoubleType(), True),
        types.StructField('TotalPrice', types.DoubleType(), True),
        types.StructField('SalesDate', types.LongType(), True),
        types.StructField('TransactionNumber', types.StringType(), True)
    ])
    sales = spark.read.option("header", True).schema(sales_schema).parquet(f"{source_path}/sales.parquet")
    sales = sales.withColumn("SalesDateSeconds", (col("SalesDate") / 1_000_000_000).cast("double"))

    # Convert to timestamp
    sales = sales.withColumn("SalesTimestamp", from_unixtime(col("SalesDateSeconds")).cast("timestamp"))
    sales = sales \
        .select("SalesID", "SalesPersonID", "CustomerID", "ProductID", "Quantity", "Discount", "TotalPrice",
                "SalesDate", "TransactionNumber", "SalesDateSeconds", "SalesTimestamp")
    return sales


def products_df(spark):
    products_schema = types.StructType([
        types.StructField('ProductID', types.LongType(), True),
        types.StructField('ProductName', types.StringType(), True),
        types.StructField('Price', types.DoubleType(), True),
        types.StructField('CategoryID', types.LongType(), True),
        types.StructField('Class', types.StringType(), True),
        types.StructField('ModifyDate', types.LongType(), True),
        types.StructField('Resistant', types.StringType(), True),
        types.StructField('IsAllergic', types.StringType(), True),
        types.StructField('VitalityDays', types.DoubleType(), True)
    ])
    products = spark.read.option("header", True).schema(products_schema).parquet(f"{source_path}/products.parquet")
    return products


def employees_df(spark):
    employees_schema = types.StructType([
        types.StructField('EmployeeID', types.LongType(), True),
        types.StructField('FirstName', types.StringType(), True),
        types.StructField('MiddleInitial', types.StringType(), True),
        types.StructField('LastName', types.StringType(), True),
        # types.StructField('BirthDate', types.TimestampType(), True),
        types.StructField('Gender', types.StringType(), True),
        types.StructField('CityID', types.LongType(), True)
        # types.StructField('HireDate', types.TimestampType(), True)
    ])
    employees = spark.read.option("header", True).schema(employees_schema).parquet(f"{source_path}/employees.parquet")
    return employees


def customers_df(spark):
    customers_schema = types.StructType([
        types.StructField('CustomerID', types.LongType(), True),
        types.StructField('FirstName', types.StringType(), True),
        types.StructField('MiddleInitial', types.StringType(), True),
        types.StructField('LastName', types.StringType(), True),
        types.StructField('CityID', types.LongType(), True),
        types.StructField('Address', types.StringType(), True)
    ])
    customers = spark.read.option("header", True).schema(customers_schema).parquet(f"{source_path}/customers.parquet")
    return customers


def fact_sales_df(spark):
    sales = sales_df(spark)
    products = products_df(spark)
    employees = employees_df(spark)
    customers = customers_df(spark)
    print(f"sales -> {sales.show()} \n products -> {products.show()} \n "
          f"customers -> {customers.show()} \n employees -> {employees.show()}")
    fact_sales = sales.join(products, "ProductID", "left") \
        .join(customers, "CustomerID", "left") \
        .join(employees, sales["SalesPersonID"] == employees["EmployeeID"], "left") \
        .select(
        "SalesID", "SalesPersonID", "CustomerID", "ProductID", "Quantity", "Discount",
        "TotalPrice", "SalesTimestamp", "TransactionNumber", "EmployeeID"
    )
    return fact_sales


def dim_products_df(spark):
    products = products_df(spark)
    dim_product = products \
        .select("ProductID", "ProductName", "Price", "CategoryID", "Class", "ModifyDate", "Resistant", "IsAllergic",
                "VitalityDays") \
        .dropDuplicates()
    return dim_product


# ---------------------
# DIMENSION TABLE LOGIC
# ---------------------
def run_dim_products(spark):
    print("Running dim_products transformation...")
    dim_product = dim_products_df(spark)
    dim_product.write \
        .format("bigquery") \
        .option("table", f"{URL}.dim_products") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


def run_dim_customers(spark):
    print("Running dim_customer transformation...")
    customers = customers_df(spark)
    dim_customers = customers \
        .select("CustomerID", "FirstName", "MiddleInitial", "LastName", "CityID", "Address") \
        .dropDuplicates()
    dim_customers.write \
        .format("bigquery") \
        .option("table", f"{URL}.dim_customers") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


def run_dim_employees(spark):
    print("Running dim_employee transformation...")
    employees = employees_df(spark)
    dim_employee = employees \
        .select("EmployeeID", "FirstName", "MiddleInitial", "LastName", "Gender", "CityID") \
        .dropDuplicates()
    dim_employee.write \
        .format("bigquery") \
        .option("table", f"{URL}.dim_employees") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


def run_dim_dates(spark):
    print("Running dim_date transformation...")
    sales = sales_df(spark)
    dim_date = sales.select("SalesTimestamp") \
        .dropDuplicates() \
        .withColumn("year", year("SalesTimestamp")) \
        .withColumn("month", month("SalesTimestamp")) \
        .withColumn("week", weekofyear("SalesTimestamp")) \
        .withColumn("day_of_month", dayofmonth("SalesTimestamp")) \
        .withColumn("day_of_week", dayofweek("SalesTimestamp")) \
        .withColumn("quarter", quarter("SalesTimestamp"))
    dim_date.write \
        .format("bigquery") \
        .option("table", f"{URL}.dim_dates") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


def run_dim_categories(spark):
    print("Running dim_categories transformation...")
    categories_schema = types.StructType([
        types.StructField('CategoryID', types.LongType(), True),
        types.StructField('CategoryName', types.StringType(), True)
    ])
    categories = spark.read.option("header", True).schema(categories_schema).parquet(
        f"{source_path}/categories.parquet")
    dim_categories = categories \
        .select("CategoryID", "CategoryName") \
        .dropDuplicates()
    dim_categories.write \
        .format("bigquery") \
        .option("table", f"{URL}.dim_categories") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


def run_dim_locations(spark):
    print("Running dim_locations transformation...")
    cities_schema = types.StructType([
        types.StructField('CityID', types.LongType(), True),
        types.StructField('CityName', types.StringType(), True),
        types.StructField('Zipcode', types.LongType(), True),
        types.StructField('CountryID', types.LongType(), True)
    ])
    countries_schema = types.StructType([
        types.StructField('CountryID', types.LongType(), True),
        types.StructField('CountryName', types.StringType(), True),
        types.StructField('CountryCode', types.StringType(), True)
    ])
    cities = spark.read.option("header", True).schema(cities_schema).parquet(f"{source_path}/cities.parquet")
    countries = spark.read.option("header", True).schema(countries_schema).parquet(f"{source_path}/countries.parquet")
    dim_location = cities.join(countries, "CountryID", "left") \
        .select("CityID", "CityName", "Zipcode", "CountryID", "CountryName", "CountryCode") \
        .dropDuplicates()
    dim_location.write \
        .format("bigquery") \
        .option("table", f"{URL}.dim_locations") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


# ----------------
# FACT TABLE LOGIC
# ----------------
def run_fact_sales(spark):
    print("Running fact_sales transformation...")
    fact_sales = fact_sales_df(spark)
    fact_sales.repartition("SalesTimestamp") \
        .write \
        .format("bigquery") \
        .option("table", f"{URL}.fact_sales") \
        .option("partitionField", "SalesTimestamp") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


# ----------------
# KPI TABLE LOGIC
# ----------------
def run_kpi_total_sales_by_date(spark):
    print("Running KPI: total sales by date...")
    fact_sales = sales_df(spark)
    sales_by_time = fact_sales.groupBy("SalesTimestamp").agg(sum("TotalPrice").alias("TotalSales"))
    sales_by_time.write.format("bigquery") \
        .option("table", f"{URL}.kpi_total_sales_by_date") \
        .option("partitionField", "SalesTimestamp") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


def run_kpi_top_products(spark):
    print("Running KPI: top products...")
    fact_sales = sales_df(spark)
    top_products = fact_sales.groupBy("ProductID").agg(sum("TotalPrice").alias("Revenue")) \
        .orderBy(col("Revenue").desc())
    top_products.write.format("bigquery") \
        .option("table", f"{URL}.kpi_top_products") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


def run_kpi_avg_order_value(spark):
    print("Running KPI: average order value...")
    fact_sales = sales_df(spark)
    avg_order_value = fact_sales.groupBy("SalesID").agg(sum("TotalPrice").alias("order_total")) \
        .agg({"order_total": "avg"})
    avg_order_value.write.format("bigquery") \
        .option("table", f"{URL}.kpi_avg_order_value") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


def run_kpi_customer_type_counts(spark):
    print("Running KPI: customer type counts...")
    fact_sales = sales_df(spark)
    first_purchase = fact_sales.groupBy("CustomerID").agg(min("SalesTimestamp").alias("FirstPurchaseDate"))

    sales_with_customer_status = fact_sales.join(first_purchase, "CustomerID") \
        .withColumn("CustomerType",
                    when(col("SalesTimestamp") > col("FirstPurchaseDate"), lit("Repeat")).otherwise(lit("New")))

    customer_type_counts = sales_with_customer_status.groupBy("SalesTimestamp", "CustomerType") \
        .agg(countDistinct("CustomerID").alias("CustomerCount"))

    customer_type_counts.write.format("bigquery") \
        .option("table", f"{URL}.kpi_customer_type_counts") \
        .option("partitionField", "SalesTimestamp") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


def run_kpi_price_trends(spark):
    print("Running KPI: price trends...")
    dim_products = dim_products_df(spark)
    price_trends = dim_products.withColumn("month", month("ModifyDate")) \
        .groupBy("CategoryID", "month") \
        .agg(avg("Price").alias("avg_price"))

    price_trends.write.format("bigquery") \
        .option("table", f"{URL}.kpi_price_trends") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


def run_kpi_sales_by_employee(spark):
    print("Running KPI: sales by employee...")
    fact_sales = sales_df(spark)
    sales_by_employee = fact_sales.groupBy("EmployeeID") \
        .agg(sum("TotalPrice").alias("employee_sales"))

    sales_by_employee.write.format("bigquery") \
        .option("table", f"{URL}.kpi_sales_by_employee") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


def run_kpi_employee_daily_sales(spark):
    print("Running KPI: employee daily sales...")
    fact_sales = sales_df(spark)
    employee_daily_sales = fact_sales.groupBy("EmployeeID", "SalesTimestamp") \
        .agg(sum("TotalPrice").alias("daily_revenue"))

    employee_daily_sales.write.format("bigquery") \
        .option("table", f"{URL}.kpi_employee_daily_sales") \
        .option("partitionField", "SalesTimestamp") \
        .option("temporaryGcsBucket", temp_path) \
        .mode("overwrite") \
        .save()


# ----------------
# MAIN ENTRYPOINT
# ----------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Grocery Data Spark Transformations")
    parser.add_argument("--task", required=True, help="Transformation task to run")

    args = parser.parse_args()
    task = args.task

    spark = create_spark_session()

    task_map = {
        "dim_products": run_dim_products,
        "dim_customers": run_dim_customers,
        "dim_employees": run_dim_employees,
        "dim_dates": run_dim_dates,
        "dim_categories": run_dim_categories,
        "dim_locations": run_dim_locations,
        "fact_sales": run_fact_sales,
        "kpi_total_sales_by_date": run_kpi_total_sales_by_date,
        "kpi_top_products": run_kpi_top_products,
        "kpi_avg_order_value": run_kpi_avg_order_value,
        "kpi_customer_type_counts": run_kpi_customer_type_counts,
        "kpi_price_trends": run_kpi_price_trends,
        "kpi_sales_by_employee": run_kpi_sales_by_employee,
        "kpi_employee_daily_sales": run_kpi_employee_daily_sales,
    }

    if task not in task_map:
        print(f"Unknown task: {task}")
    else:
        task_map[task](spark)
        spark.stop()
