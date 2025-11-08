# Bronze Layer - Raw Data Ingestion

## Overview
The Bronze layer contains **raw, unprocessed data** ingested from CSV files into Parquet format. This layer preserves data exactly as received from source systems with minimal transformation (schema enforcement only).

## Tables

### 1. **population**
- **Source**: `Telco_customer_churn_population.csv`
- **Purpose**: Zip code population data
- **Key Columns**: `zip_code`, `population`

### 2. **demographics**
- **Source**: `Telco_customer_churn_demographics.csv`
- **Purpose**: Customer demographic information
- **Key Columns**: `customer_id`, `gender`, `age`, `married`, `dependents`

### 3. **status**
- **Source**: `Telco_customer_churn_status.csv`
- **Purpose**: Customer churn status and satisfaction scores
- **Key Columns**: `customer_id`, `quarter`, `customer_status`, `churn_category`, `churn_reason`

### 4. **location**
- **Source**: `Telco_customer_churn_location.csv`
- **Purpose**: Customer geographic location
- **Key Columns**: `customer_id`, `city`, `zip_code`, `latitude`, `longitude`

### 5. **services**
- **Source**: `Telco_customer_churn_services.csv`
- **Purpose**: Customer service subscriptions and billing
- **Key Columns**: `customer_id`, `quarter`, `phone_service`, `internet_service`, `contract`, `monthly_charge`

## Execution Order

Run scripts in this order:

```bash
# 1. Create external tables in Hive
spark-submit ddl_bronze.py

# 2. Load CSV data to Parquet
spark-submit load.py

# 3. Validate data quality
spark-submit validate.py
```

## Storage

- **Format**: Parquet (converted from CSV)
- **Location**: `hdfs://namenode:9000/user/data/telco/{table}/parquet/`
- **Database**: `bronze_telco`
- **Table Type**: External tables (data managed separately from Hive)

## Data Quality

All tables include metadata columns:
- `ingestion_timestamp`: When the record was loaded
- `source_file`: Original CSV filename

The `validate.py` script checks:
- Row counts
- Null values in critical columns
- Data type consistency
- Referential integrity
