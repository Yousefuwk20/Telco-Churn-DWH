"""
===============================================================================
DDL Script: Create Bronze Layer External Tables
===============================================================================
Script Purpose:
    This script creates external tables in the 'bronze_telco' database
    pointing to existing Parquet files on HDFS. Tables are created only
    if they don't already exist.
    
    - Creates bronze_telco database if not exists
    - Creates 5 external tables (population, demographics, status, location, services)
    - Refreshes table metadata to sync with HDFS files
    - Tables point to: hdfs:///user/data/telco/{table}/parquet/
    
Usage:
    spark-submit ddl_bronze.py
===============================================================================
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bronze Layer Schema") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE DATABASE IF NOT EXISTS bronze_telco")
print("Database bronze_telco created/verified")
spark.catalog.setCurrentDatabase("bronze_telco")

# Population table
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_telco.population (
    id INT,
    zip_code INT,
    population STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///user/data/telco/population/parquet/';
""")
spark.sql("REFRESH TABLE bronze_telco.population")
print("Table bronze_telco.population created")

# Demographics table
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_telco.demographics (
    customer_id STRING,
    count INT,
    gender STRING,
    age INT,
    under_30 STRING,
    senior_citizen STRING,
    married STRING,
    dependents STRING,
    number_of_dependents INT,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///user/data/telco/demographics/parquet/';
""")
spark.sql("REFRESH TABLE bronze_telco.demographics")
print("Table bronze_telco.demographics created")

# Status table
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_telco.status (
    customer_id STRING,
    count INT,
    quarter STRING,
    satisfaction_score INT,
    customer_status STRING,
    churn_label STRING,
    churn_value INT,
    churn_score INT,
    cltv INT,
    churn_category STRING,
    churn_reason STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///user/data/telco/status/parquet/';
""")
spark.sql("REFRESH TABLE bronze_telco.status")
print("Table bronze_telco.status created")

# Location table
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_telco.location (
    customer_id STRING,
    count INT,
    country STRING,
    state STRING,
    city STRING,
    zip_code INT,
    lat_long STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///user/data/telco/location/parquet/';
""")
spark.sql("REFRESH TABLE bronze_telco.location")
print("Table bronze_telco.location created")

# Services table
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_telco.services (
    customer_id STRING,
    count INT,
    quarter STRING,
    referred_a_friend STRING,
    number_of_referrals INT,
    tenure_in_months INT,
    offer STRING,
    phone_service STRING,
    avg_monthly_long_distance_charges DOUBLE,
    multiple_lines STRING,
    internet_service STRING,
    internet_type STRING,
    avg_monthly_gb_download INT,
    online_security STRING,
    online_backup STRING,
    device_protection_plan STRING,
    premium_tech_support STRING,
    streaming_tv STRING,
    streaming_movies STRING,
    streaming_music STRING,
    unlimited_data STRING,
    contract STRING,
    paperless_billing STRING,
    payment_method STRING,
    monthly_charge DOUBLE,
    total_charges DOUBLE,
    total_refunds DOUBLE,
    total_extra_data_charges INT,
    total_long_distance_charges DOUBLE,
    total_revenue DOUBLE,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///user/data/telco/services/parquet/';
""")

spark.sql("REFRESH TABLE bronze_telco.services")
print("Table bronze_telco.services created")

print("\nBronze layer schema created successfully")
print("All tables registered in Hive Metastore")
spark.stop()

