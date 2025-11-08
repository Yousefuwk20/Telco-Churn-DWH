# Silver Layer DAG

## Overview
Daily ETL pipeline for the Silver layer of the Telco Customer Churn Data Warehouse. Loads 6 conformed dimension tables and validates data quality.

### Tasks

1. **load_dim_customer**
   - **Type:** SparkSubmitOperator
   - **Script:** `/root/airflow/dags/scripts/silver/dimensions/dim_customer.py`
   - **Purpose:** Creates customer dimension
   - **Output:** `silver_telco.dim_customer` table in HDFS

2. **load_dim_service_package**
   - **Type:** SparkSubmitOperator
   - **Script:** `/root/airflow/dags/scripts/silver/dimensions/dim_service_package.py`
   - **Purpose:** Creates service dimension
   - **Output:** `silver_telco.dim_service_package` table in HDFS

3. **load_dim_location**
   - **Type:** SparkSubmitOperator
   - **Script:** `/root/airflow/dags/scripts/silver/dimensions/dim_location.py`
   - **Purpose:** Creates geographic dimension with city/state/zip
   - **Output:** `silver_telco.dim_location` table in HDFS

4. **load_dim_churn_status**
   - **Type:** SparkSubmitOperator
   - **Script:** `/root/airflow/dags/scripts/silver/dimensions/dim_churn_status.py`
   - **Purpose:** Creates churn status mini-dimension
   - **Output:** `silver_telco.dim_churn_status` table in HDFS

5. **load_dim_quarter**
   - **Type:** SparkSubmitOperator
   - **Script:** `/root/airflow/dags/scripts/silver/dimensions/dim_quarter.py`
   - **Purpose:** Creates time dimension (quarter grain)
   - **Output:** `silver_telco.dim_quarter` table in HDFS

6. **load_dim_promotion**
   - **Type:** SparkSubmitOperator
   - **Script:** `/root/airflow/dags/scripts/silver/dimensions/dim_promotion.py`
   - **Purpose:** Creates promotional offer mini-dimension
   - **Output:** `silver_telco.dim_promotion` table in HDFS

7. **validate_dimensions**
   - **Type:** @task 
   - **Purpose:** Validates all 6 dimension tables have data
   - **Method:** HiveCliHook with COUNT(*) queries
   - **Validation Rules:** Each dimension must have > 0 rows

8. **log_completion**
   - **Type:** @task 
   - **Purpose:** Logs pipeline completion timestamp

## Schedule

- **Frequency:** Daily at midnight (`@daily`)
- **Start Date:** 2025-11-08
- **Catchup:** Disabled


## Usage

```bash
docker exec namenode airflow dags trigger silver_telco_daily
```
