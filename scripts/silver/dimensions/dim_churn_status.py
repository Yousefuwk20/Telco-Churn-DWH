"""
===============================================================================
Silver Layer - dim_churn_status Transformation
===============================================================================
Purpose:
    Creates churn status mini-dimension with unique combinations of status attributes
    This is a low-cardinality dimension for time-variant attributes

Source Tables:
    - bronze_telco.status

Target Table:
    - silver_telco.dim_churn_status

Grain:
    One row per churn status 

Transformations:
    - Standardize customer status values
    - Clean and standardize churn categories
    - Clean churn reasons
    - Generate unique combinations
    - Assign surrogate keys

Usage:
    spark-submit dim_churn_status.py
===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, lit, trim, initcap, coalesce, xxhash64
)

spark = SparkSession.builder \
    .appName("Silver - dim_churn_status") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")


# =============================================================================
# Read Bronze Status Table
# =============================================================================

status = spark.table("bronze_telco.status")
print(f"Status records: {status.count()} rows")

# =============================================================================
# Clean and Standardize Status Attributes
# =============================================================================

status_clean = status \
    .withColumn('customer_status_raw', trim(initcap(col('customer_status')))) \
    .withColumn('churn_category_raw', trim(initcap(col('churn_category')))) \
    .withColumn('churn_reason_raw', trim(col('churn_reason')))

# Standardize Customer Status
status_clean = status_clean.withColumn(
    'customer_status',
    when(col('customer_status_raw').isin(['Churned', 'Churn']), 'Churned')
    .when(col('customer_status_raw').isin(['Stayed', 'Stay', 'Active', 'Retained']), 'Active')
    .when(col('customer_status_raw').isin(['Joined', 'New']), 'New')
    .otherwise('n/a')
)

# Clean Churn Category (n/a for non-churned customers)
status_clean = status_clean.withColumn(
    'churn_category',
    when(col('customer_status') == 'Churned', 
         coalesce(col('churn_category_raw'), lit('n/a')))
    .otherwise(lit('n/a'))
)

# Clean Churn Reason (n/a for non-churned customers)
status_clean = status_clean.withColumn(
    'churn_reason',
    when(col('customer_status') == 'Churned', 
         coalesce(col('churn_reason_raw'), lit('n/a')))
    .otherwise(lit('n/a'))
)

print(f"Cleaned status records: {status_clean.count()} rows")

# =============================================================================
# Get Unique Combinations
# =============================================================================

dim_churn_status = status_clean.select(
    'customer_status',
    'churn_category',
    'churn_reason'
).distinct()

unique_count = dim_churn_status.count()
print(f"Unique combinations: {unique_count}")

# =============================================================================
# Generate Surrogate Keys (Hash-based for stability)
# =============================================================================

dim_churn_status = dim_churn_status.withColumn(
    'churn_status_key',
    xxhash64(
        col('customer_status'), 
        col('churn_category'), 
        col('churn_reason')
    )
)

# =============================================================================
# Select Final Columns
# =============================================================================
dim_churn_status_final = dim_churn_status.select(
    'churn_status_key',
    'customer_status',
    'churn_category',
    'churn_reason'
)

# =============================================================================
# Write to Silver Layer
# =============================================================================

dim_churn_status_final.write \
    .mode('overwrite') \
    .format('parquet') \
    .saveAsTable('silver_telco.dim_churn_status')

final_count = spark.table('silver_telco.dim_churn_status').count()
print(f"Written to silver_telco.dim_churn_status: {final_count} rows")

spark.stop()

print("dim_churn_status transformation completed successfully!")
