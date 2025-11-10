"""
===============================================================================
Silver Layer - facts_base Transformation
===============================================================================
Purpose:
    Creates the facts base table with ALL cleaned measures from Bronze
    This is the single source of truth for fact measures - cleaned once, used by all Gold tables
    
Source Tables:
    - bronze_telco.status (churn metrics, satisfaction, CLTV)
    - bronze_telco.services (tenure, charges, service usage metrics)

Target Table:
    - silver_telco.facts_base

Grain:
    One row per customer per quarter: (customer_id, quarter)
    
Measures Included:
    Raw measures only - no derived calculations, no dimensional flags
    Customer Metrics: tenure, satisfaction, churn scores, CLTV
    Financial Measures: monthly charges, total charges, revenue, refunds
    Service Usage: data download, long distance charges, referrals

Usage:
    spark-submit facts_base.py
===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, trim
)

spark = SparkSession.builder \
    .appName("Silver - facts_base") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")

# =============================================================================
# Read Bronze Tables
# =============================================================================

bronze_status = spark.table("bronze_telco.status")
bronze_services = spark.table("bronze_telco.services")

print(f"   Status records: {bronze_status.count()} rows")
print(f"   Services records: {bronze_services.count()} rows")

# =============================================================================
# Join Bronze Tables
# =============================================================================

facts_raw = bronze_status.join(
    bronze_services,
    bronze_status.customer_id == bronze_services.customer_id,
    'inner'
).drop(bronze_services.customer_id).drop(bronze_services.quarter)

joined_count = facts_raw.count()
print(f"   Joined records: {joined_count} rows")

# =============================================================================
# Clean and Transform Measures
# =============================================================================

facts_clean = facts_raw.select(
    trim(col('customer_id')).alias('customer_id'),
    trim(col('quarter')).alias('quarter'),
    
    # Raw Measures - Customer Tenure & Lifecycle
    col('tenure_in_months').cast('int').alias('tenure_months'),
    col('satisfaction_score').cast('int').alias('satisfaction_score'),
    
    # Raw Measures - Churn Metrics
    col('churn_value').cast('int').alias('churn_value'),
    col('churn_score').cast('int').alias('churn_score'),
    col('cltv').cast('int').alias('cltv'),
    
    # Raw Measures - Financial (Monthly)
    col('monthly_charge').cast('decimal(10,2)').alias('monthly_charges'),
    col('avg_monthly_long_distance_charges').cast('decimal(10,2)').alias('avg_monthly_long_distance_charges'),
    
    # Raw Measures - Financial (Total/Lifetime)
    col('total_charges').cast('decimal(10,2)').alias('total_charges'),
    col('total_revenue').cast('decimal(10,2)').alias('total_revenue'),
    col('total_refunds').cast('decimal(10,2)').alias('total_refunds'),
    col('total_extra_data_charges').cast('decimal(10,2)').alias('total_extra_data_charges'),
    col('total_long_distance_charges').cast('decimal(10,2)').alias('total_long_distance_charges'),
    
    # Raw Measures - Service Usage
    col('avg_monthly_gb_download').cast('int').alias('avg_monthly_gb_download'),
    col('number_of_referrals').cast('int').alias('number_of_referrals')
)

# =============================================================================
# Data Quality Checks
# =============================================================================

duplicate_count = facts_clean.groupBy('customer_id', 'quarter').count() \
    .filter(col('count') > 1).count()
print(f"   Duplicate (customer_id, quarter) combinations: {duplicate_count}")

null_checks = facts_clean.select([
    F.sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in ['customer_id', 'quarter']
])
print("\n   NULL counts in grain columns:")
null_checks.show()

print("\n   Measure statistics:")
facts_clean.select(
    'tenure_months', 'monthly_charges', 'total_revenue', 'cltv'
).describe().show()

# =============================================================================
# Write to Silver Layer
# =============================================================================

facts_clean.write \
    .mode('overwrite') \
    .format('parquet') \
    .saveAsTable('silver_telco.facts_base')

final_count = spark.table('silver_telco.facts_base').count()
print(f"   Written to silver_telco.facts_base: {final_count} rows")


spark.stop()
