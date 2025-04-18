# 🛒 Grocery Sales Data Pipeline

🚨 **NOTE**: This project is hosted on a temporary GCP environment. **The dashboards may become unavailable after the cloud credits expire.** 🚨

---

## 📦 Overview

This project is an end-to-end data pipeline that ingests, transforms, and visualizes grocery sales data using:

- **Apache Airflow** for orchestration  
- **Apache Spark** for transformations  
- **Google Cloud Platform (GCS & BigQuery)** for storage and querying  
- **Looker Studio** for visualization  

---

## 📁 Project Structure

```
.
├── airflow/
│   └── dags/
│       ├── data_ingestion_gcs_dag.py
│       ├── spark_grocery_transformations_dag.py
├── credentials/
│   └── google_credentials.json
├── terraform/
│   ├── main.tf
│   └── variables.tf
├── .env.example
├── docker-compose.yml
├── Dockerfile
├── README.md
```

---

## ⚙️ Setup Instructions

### 1. Prerequisites

- [Docker](https://docs.docker.com/get-docker/) installed  
- [Terraform](https://developer.hashicorp.com/terraform/install) installed  
- GCP project with:
  - BigQuery and GCS enabled  
  - A service account with the following roles:
    - `BigQuery Admin`
    - `Storage Admin`

---

### 2. Configure Environment Variables

#### 🔐 Create `.env` File

```bash
cp .env.example .env
```

Then edit `.env` and populate with your actual values:

```env
# Terraform
TF_VAR_project=your-gcp-project-id
TF_VAR_region=us-central1
TF_VAR_location=US
TF_VAR_zone=us-central1-a
TF_VAR_bq_dataset_name=grocery_sales_pipeline
TF_VAR_gcs_storage=your-gcs-bucket-name

# Airflow
AIRFLOW_PROJ_DIR=./airflow
AIRFLOW_UID=501

GCP_PROJECT_ID=your-gcp-project-id
GCP_GCS_BUCKET=your-gcs-bucket-name
BIGQUERY_DATASET=grocery_sales_pipeline

AIRFLOW_CONN_SPARK='{
  "conn_type": "spark",
  "host": "spark://spark-master",
  "port": 7077
}'
```

#### 🔐 Add GCP Credentials

Place your GCP service account key in:

```bash
credentials/google_credentials.json
```

---

### 3. Provision GCP Resources (Terraform)

Navigate to the `terraform` folder and run:

```bash
cd terraform
terraform init
terraform apply
```

> This will create:
> - GCS bucket for raw and processed data
> - BigQuery dataset for storing external and transformed tables

---

### 4. Launch Airflow + Spark (Docker)

```bash
docker-compose up
```

Once services are running, visit:

- Airflow UI: [http://localhost:8080](http://localhost:8080)

Login with the default credentials:
```
Username: airflow
Password: airflow
```

- Monitor Spark Applications: [http://localhost:8081](http://localhost:8081)

---

## 🚦 DAG Execution Order

1. `data_ingestion_gcs_dag`
   - Downloads raw CSVs
   - Uploads them to GCS
   - Creates BigQuery external tables

2. `spark_grocery_transformations_dag`
   - Reads from GCS
   - Transforms with PySpark
   - Writes Parquet + transformed tables into BigQuery

> 🛑 `hello_spark_dag` is for testing only — not part of the production pipeline.

---

## 📊 Looker Studio Dashboards

Connect to the following tables in BigQuery for visualization:

- `kpi_top_products`
- `kpi_total_sales_by_date`

### Suggested Charts

#### 📈 Sales Over Time

- **Chart**: Time Series  
- **Dimension**: `sale_date`  
- **Metric**: `total_sales`  

#### 🥇 Top Products

- **Chart**: Bar Chart  
- **Dimension**: `product_name`  
- **Metric**: `total_sales`  

#### 🧺 Quantity vs Sales

- **Chart**: Scatter or Combo  
- **Dimensions**: `product_name`  
- **Metrics**: `quantity_sold`, `total_sales`

> 📌 Screenshots of dashboards go here:

![Daily Sales Trend](screenshots/daily_sales_trend.png)  
![Top Products](screenshots/top_products_bar_chart.png)

---

## 📜 License

[MIT License](./LICENSE)

---

## 🙌 Credits

Made with ❤️ using open data, open source, and cloud tools.
