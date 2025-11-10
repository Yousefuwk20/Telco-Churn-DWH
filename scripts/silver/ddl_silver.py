"""
===============================================================================
Silver Layer DDL - Create Conformed Dimension Tables
===============================================================================
Purpose:
    Creates conformed dimension tables in Silver layer.
    Silver layer contains cleaned, standardized, reusable dimensional data.
    
Tables:
    - dim_customer: Customer dimension
    - dim_service_package: Service package dimension 
    - dim_location: Location dimension with SCD Type 1
    - dim_churn_status: Churn status mini-dimension
    - dim_quarter: Quarterly time dimension
    - dim_promotion: Promotion mini-dimension
    - facts_base: Cleaned fact measures (single source of truth for Gold layer) 

Usage:
    spark-submit ddl_silver.py
===============================================================================
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Create Silver Tables") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS silver_telco")

# 1. Customer Dimension
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_telco.dim_customer (
        customer_key BIGINT COMMENT 'Surrogate key (sequential)',
        customer_id STRING COMMENT 'Natural key from source',
        age INT COMMENT 'Customer age',
        age_group STRING COMMENT '18-24, 25-34, 35-44, 45-54, 55-64, 65+',
        gender STRING COMMENT 'Male/Female/Unknown (standardized)',
        marital_status STRING COMMENT 'Married/Single/"n/a" (standardized)',
        number_of_dependents INT COMMENT 'Number of dependents',
        family_size INT COMMENT 'Total family size (customer + spouse + dependents)'
    )
    USING PARQUET
    LOCATION 'hdfs://namenode:9000/user/data/telco/silver/dim_customer'
    COMMENT 'Customer dimension with stable demographic attributes. Grain: One row per customer.'
""")

print("dim_customer created")

# 2. Service Package Dimension
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_telco.dim_service_package (
        service_package_key BIGINT COMMENT 'Surrogate key (deterministic hash)',
        
        -- Base Service Attributes
        phone_service STRING COMMENT 'Yes/No',
        multiple_lines STRING COMMENT 'Yes/No/No phone service',
        internet_service STRING COMMENT 'DSL/Fiber optic/None',
        online_security STRING COMMENT 'Yes/No/Not applicable',
        online_backup STRING COMMENT 'Yes/No/Not applicable',
        device_protection STRING COMMENT 'Yes/No/Not applicable',
        tech_support STRING COMMENT 'Yes/No/Not applicable',
        streaming_tv STRING COMMENT 'Yes/No/Not applicable',
        streaming_movies STRING COMMENT 'Yes/No/Not applicable',
        streaming_music STRING COMMENT 'Yes/No/Not applicable',
        unlimited_data STRING COMMENT 'Yes/No/Not applicable',
        
        -- Billing Attributes (included for simplicity)
        contract STRING COMMENT 'Month-to-month/One year/Two year',
        paperless_billing STRING COMMENT 'Yes/No',
        payment_method STRING COMMENT 'Electronic check/Mailed check/Bank transfer/Credit card',
        
        -- Derived Attributes (pre-calculated for analysis)
        total_services INT COMMENT 'Count of add-on services (0-8)',
        service_tier STRING COMMENT 'Basic/Standard/Premium (based on add-on count)',
        bundle_type STRING COMMENT 'Double play/Single service/No service'
    )
    USING PARQUET
    LOCATION 'hdfs://namenode:9000/user/data/telco/silver/dim_service_package'
    COMMENT 'Service package dimension. Grain: One row per unique combination of services/billing attributes.'
""")

print("dim_service_package created")

# 3. Location Dimension (SCD Type 1)
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_telco.dim_location (
        location_key BIGINT COMMENT 'Surrogate key (sequential)',
        city STRING COMMENT 'City name (standardized with title case)',
        zip_code STRING COMMENT 'Zip code (5 digits with leading zeros)',
        latitude DOUBLE COMMENT 'Geographic latitude (validated, rounded to 4 decimals)',
        longitude DOUBLE COMMENT 'Geographic longitude (validated, rounded to 4 decimals)',
        population INT COMMENT 'Zip Code population (cleaned)',
        population_density STRING COMMENT 'Urban/Suburban/Rural (derived from population)'
    )
    USING PARQUET
    LOCATION 'hdfs://namenode:9000/user/data/telco/silver/dim_location'
    COMMENT 'Location dimension with geographic and demographic attributes (SCD Type 1)'
""")

print("dim_location created")

# 4. Churn Status Mini-Dimension (Low-Cardinality Attributes)
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_telco.dim_churn_status (
        churn_status_key BIGINT COMMENT 'Surrogate key (deterministic hash)',
        customer_status STRING COMMENT 'Active/Churned/New (standardized)',
        churn_category STRING COMMENT 'Competitor/Dissatisfaction/Attitude/Price/Other/"n/a"',
        churn_reason STRING COMMENT 'Specific reason for churn (NULL if not churned)'
    )
    USING PARQUET
    LOCATION 'hdfs://namenode:9000/user/data/telco/silver/dim_churn_status'
    COMMENT 'Churn status mini-dimension - one row per unique status combination'
""")

print("dim_churn_status created")

# 5. Quarter Dimension (Time Context for Snapshots)
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_telco.dim_quarter (
        quarter_key INT COMMENT 'Surrogate key (YYYYQQ format), e.g., 202503',
        quarter_name STRING COMMENT 'Business-friendly name, e.g., Q3-2025',
        year INT COMMENT 'Year component',
        quarter_of_year INT COMMENT 'Quarter number (1-4)',
        half_of_year STRING COMMENT 'Half of the year (H1 or H2)'
    )
    USING PARQUET
    LOCATION 'hdfs://namenode:9000/user/data/telco/silver/dim_quarter'
    COMMENT 'Quarterly time dimension - one row per unique quarter (e.g., Q3-2025)'
""")

print("dim_quarter created")

# 6. Promotion Mini-Dimension (Offers and Referrals)
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_telco.dim_promotion (
        promotion_key BIGINT COMMENT 'Surrogate key (deterministic hash)',
        offer STRING COMMENT 'Promotional offer (e.g., Offer A, Offer B, None)',
        referred_a_friend STRING COMMENT 'Whether customer referred a friend (Yes/No)'
    )
    USING PARQUET
    LOCATION 'hdfs://namenode:9000/user/data/telco/silver/dim_promotion'
    COMMENT 'Promotion mini-dimension. Grain: One row per unique offer/referral combination.'
""")

print("dim_promotion created")

# 7. Facts Base Table (Cleaned Measures - Single Source of Truth)
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_telco.facts_base (
        -- Natural Keys (Define the grain)
        customer_id STRING COMMENT 'Natural key for customer',
        quarter STRING COMMENT 'Natural key for time snapshot (e.g., Q3-2025)',
        
        -- Raw Measures - Customer Tenure & Lifecycle
        tenure_months INT COMMENT 'Tenure in months',
        satisfaction_score INT COMMENT 'Satisfaction score (1-5)',
        
        -- Raw Measures - Churn Metrics
        churn_value INT COMMENT 'Churn value (0=Active, 1=Churned)',
        churn_score INT COMMENT 'Churn risk score (0-100)',
        cltv INT COMMENT 'Customer Lifetime Value',
        
        -- Raw Measures - Financial (Monthly)
        monthly_charges DECIMAL(10,2) COMMENT 'Monthly charges in dollars',
        avg_monthly_long_distance_charges DECIMAL(10,2) COMMENT 'Average monthly long distance charges',
        
        -- Raw Measures - Financial (Total/Lifetime)
        total_charges DECIMAL(10,2) COMMENT 'Total charges (lifetime)',
        total_revenue DECIMAL(10,2) COMMENT 'Total revenue generated',
        total_refunds DECIMAL(10,2) COMMENT 'Total refunds issued',
        total_extra_data_charges DECIMAL(10,2) COMMENT 'Total extra data charges',
        total_long_distance_charges DECIMAL(10,2) COMMENT 'Total long distance charges',
        
        -- Raw Measures - Service Usage
        avg_monthly_gb_download INT COMMENT 'Average monthly GB download',
        number_of_referrals INT COMMENT 'Number of friend referrals'
    )
    USING PARQUET
    LOCATION 'hdfs://namenode:9000/user/data/telco/silver/facts_base'
    COMMENT 'Base fact table with cleaned raw measures. Grain: (customer_id, quarter). Single source of truth for Gold layer.'
""")

print("facts_base created")

# Refresh tables to update metadata
tables = ['dim_customer', 'dim_service_package', 'dim_location', 'dim_churn_status', 'dim_quarter', 'dim_promotion', 'facts_base']

for table in tables:
    spark.sql(f"REFRESH TABLE silver_telco.{table}")
    print(f"Refreshed silver_telco.{table}")

spark.stop()

print("\nAll Silver layer dimension tables created successfully!")

