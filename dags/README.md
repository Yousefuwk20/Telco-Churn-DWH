# Bronze Layer DAG

## Overview
Daily ETL pipeline for the Bronze layer of the Telco Customer Churn Data Warehouse. Loads raw CSV files into Parquet format and validates data quality.

## Pipeline Flow

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│  Load Data  │ ───> │   Validate   │ ───> │ Log Success │
│   (Spark)   │      │   (Spark)    │      │  (Python)   │
└─────────────┘      └──────────────┘      └─────────────┘
```

### Tasks

1. **load_bronze_data**
   - **Type:** SparkSubmitOperator
   - **Script:** `/root/airflow/dags/scripts/bronze/load.py`
   - **Purpose:** Reads 5 CSV files, adds metadata columns, writes to HDFS as Parquet
   - **Output:** Parquet files in `hdfs://namenode:9000/user/data/telco/bronze/`

2. **validate_bronze_data**
   - **Type:** SparkSubmitOperator
   - **Script:** `/root/airflow/dags/scripts/bronze/validate.py`
   - **Purpose:** Validates row counts in Bronze tables against expected thresholds
   - **Validation Rules:**
     - demographics: ≥ 7,000 rows
     - location: ≥ 7,000 rows
     - population: ≥ 1,600 rows
     - services: ≥ 7,000 rows
     - status: ≥ 7,000 rows

3. **log_success**
   - **Type:** @task (Python function)
   - **Purpose:** Logs pipeline completion timestamp

## Schedule

- **Frequency:** Daily at midnight (`@daily`)
- **Start Date:** 2025-10-28
- **Catchup:** Disabled

## Prerequisites

### 1. Bronze Tables Must Exist
Run the one-time setup to create external tables:

### 2. CSV Files Must Be Available
Ensure source CSV files are accessible:
- Telco_customer_churn_demographics.csv
- Telco_customer_churn_location.csv
- Telco_customer_churn_population.csv
- Telco_customer_churn_services.csv
- Telco_customer_churn_status.csv

### 3. Spark Connection
Create Airflow connection for local Spark:
```bash
airflow connections add spark_local \
  --conn-type spark \
  --conn-host local[*]
```

## Usage

```bash
docker exec namenode airflow dags trigger bronze_telco_daily
```