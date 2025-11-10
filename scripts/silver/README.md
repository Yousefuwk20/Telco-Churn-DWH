# Silver Layer

## Overview
The Silver layer contains **cleaned, standardized, and reusable dimension tables** for the Telco Churn data warehouse. These dimensions are conformed and can be referenced by multiple fact tables in the Gold layer.

## Tables

### 1. **dim_customer**
- **Grain**: One row per customer
- **Type**: Standard dimension
- **Key**: `customer_key` (BIGINT)
- **Attributes**: Demographics (age, gender, marital status, dependents)
- **SCD**: Type 0 (no history tracking - stable attributes only)

### 2. **dim_service_package**
- **Grain**: One row per unique service combination
- **Type**: Standard dimension
- **Key**: `service_package_key` (BIGINT hash-based)
- **Attributes**: Services (phone, internet, streaming, etc.), contract, billing

### 3. **dim_location**
- **Grain**: One row per city/zip code
- **Type**: Geographic dimension
- **Key**: `location_key` (BIGINT)
- **Attributes**: City, zip code, coordinates, population
- **SCD**: Type 1 (overwrite changes)

### 4. **dim_churn_status**
- **Grain**: One row per unique status combination
- **Type**: Mini-dimension
- **Key**: `churn_status_key` (BIGINT hash-based)
- **Attributes**: Customer status, churn category, churn reason

### 5. **dim_quarter**
- **Grain**: One row per quarter
- **Type**: Time dimension
- **Key**: `quarter_key` (INT in YYYYQQ format, e.g., 202503)
- **Attributes**: Quarter name, year, quarter number, half

### 6. **dim_promotion**
- **Grain**: One row per unique offer/referral combination
- **Type**: Mini-dimension
- **Key**: `promotion_key` (BIGINT hash-based)
- **Attributes**: Offer type, referred a friend status

### 7. **facts_base**
- **Grain**: One row per customer per quarter: (customer_id, quarter)
- **Type**: Base fact table (single source of truth for measures)
- **Keys**: `customer_id` (STRING), `quarter` (STRING) - natural keys
- **Measures**: 14 raw measures including tenure, satisfaction, churn metrics, financial measures, service usage
- **Purpose**: Cleaned measures used by all Gold layer fact tables
- **Design**: Raw truth only - no derived calculations, no dimensional attributes

## Execution Order

Run scripts in this order:

```bash
# 1. Create dimension tables
spark-submit ddl_silver.py

# 2. Load dimensions (in any order)
spark-submit dimensions/dim_customer.py
spark-submit dimensions/dim_location.py
spark-submit dimensions/dim_service_package.py
spark-submit dimensions/dim_churn_status.py
spark-submit dimensions/dim_quarter.py
spark-submit dimensions/dim_promotion.py

# 3. Load facts_base
spark-submit dimensions/facts_base.py
```

## Key Design Decisions

- **Hash-based keys**: `dim_service_package`, `dim_churn_status`, and `dim_promotion` use deterministic hashing to prevent key corruption on reloads
- **Mini-dimensions**: Low-cardinality time-variant attributes separated into dedicated dimensions
- **No time-variance in customer**: Customer dimension only contains stable demographic attributes
- **Conformed dimensions**: All dimensions use standardized values (e.g.,"Yes/No/'n/a'")

## Storage

- **Format**: Parquet
- **Location**: `hdfs://namenode:9000/user/data/telco/silver/`
- **Database**: `silver_telco`
