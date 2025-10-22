"""
===============================================================================
Bronze Layer Data Loading Script
===============================================================================
Script Purpose:
    This script loads Telco customer data from CSV files on HDFS and converts
    them to Parquet format for the Bronze Layer. This is a full-load operation
    that overwrites existing data.
    
Process:
    1. Reads CSV files from HDFS (with header and schema inference)
    2. Adds metadata columns (ingestion_timestamp, source_file)
    3. Writes data as Parquet with Snappy compression
    4. Overwrites existing Parquet files in target location
    
Input:
    - CSV files in: hdfs://namenode:9000/user/data/telco/*.csv
    - 5 source files: demographics, location, population, services, status
    
Output:
    - Parquet files in: hdfs://namenode:9000/user/data/telco/{table}/parquet/
    - Format: Parquet with Snappy compression
    - Mode: Overwrite (full load)
    
Metadata Columns Added:
    - ingestion_timestamp: Current timestamp when data was loaded
    - source_file: Name of the source CSV file
    
Usage:
    spark-submit load.py
    
Notes:
    - Run ddl_bronze.py after this to create/update table schemas
===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


spark = SparkSession.builder \
    .appName("Telco Bronze Layer Loader") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE DATABASE IF NOT EXISTS bronze_telco")

file_configs = [
    {'file': 'Telco_customer_churn_demographics.csv', 'table': 'demographics'},
    {'file': 'Telco_customer_churn_location.csv', 'table': 'location'},
    {'file': 'Telco_customer_churn_population.csv', 'table': 'population'},
    {'file': 'Telco_customer_churn_services.csv', 'table': 'services'},
    {'file': 'Telco_customer_churn_status.csv', 'table': 'status'}
]

base_source_path = "hdfs://namenode:9000/user/data/telco/"
base_target_path = "hdfs://namenode:9000/user/data/telco/"

for config in file_configs:
    file_name = config['file']
    table_name = config['table']
    source_path = f"{base_source_path}{file_name}"
    target_path = f"{base_target_path}{table_name}/parquet/"

    print(f"Processing {file_name}...")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(source_path)
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_file", lit(file_name))
    
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(target_path)
    
    count = df.count()
    print(f"Loaded {count} rows to {target_path}")

print(f"\nAll tables loaded successfully")
spark.stop()