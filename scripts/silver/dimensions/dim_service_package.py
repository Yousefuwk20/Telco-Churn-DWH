"""
===============================================================================
Silver Layer - dim_service_package Transformation
===============================================================================
Purpose:
    Transforms bronze services data into a conformed service package dimension.
    
Grain:
    One row per UNIQUE COMBINATION of service attributes.
    
Key Concept:
    This dimension describes service PACKAGES, not customer subscriptions.
    If 10,000 customers have "Phone + Fiber Internet + Month-to-month",
    that combination appears ONCE in this dimension.
    
    The fact table links customers to packages via service_package_key.
    
Source:
    - bronze_telco.services
    
Target:
    - silver_telco.dim_service_package

Transformations:
    1. Data Cleaning & Standardization:
       - Standardize service values (Yes/No/Not applicable)
       - Internet service: "DSL"/"Fiber optic"/"None"
       - Contract types: "Month-to-month"/"One year"/"Two year"
       - Payment methods: Standardize formats
    
    2. Get DISTINCT combinations of all service attributes
    
    3. Derive Attributes:
       - Total service count (0-6)
       - Service tier (Basic/Standard/Premium)
       - Bundle type (Double play/Single service)
    
    4. Generate surrogate key (service_package_key)
    
Usage:
    spark-submit dim_service_package.py
===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, upper, xxhash64, concat_ws
)

spark = SparkSession.builder \
    .appName("Load dim_service_package") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")


# ============================================================================
# Read Bronze Services Data
# ============================================================================

bronze_services = spark.table("bronze_telco.services")

initial_count = bronze_services.count()
print(f"Initial record count: {initial_count:,}")
# ============================================================================
# Standardization - Clean and Standardize Values
# ============================================================================

services_clean = bronze_services.select(
    when(trim(upper(col("phone_service"))) == "YES", "Yes")
        .when(trim(upper(col("phone_service"))) == "NO", "No")
        .otherwise("n/a").alias("phone_service"),
    
    when(trim(upper(col("multiple_lines"))) == "YES", "Yes")
        .when(trim(upper(col("multiple_lines"))) == "NO", "No")
        .when(trim(upper(col("phone_service"))) == "NO", "No phone service")
        .otherwise("No phone service").alias("multiple_lines"),
    
    when(trim(upper(col("internet_service"))) == "YES", "Yes")
        .when(trim(upper(col("internet_service"))) == "NO", "None")
        .when(trim(upper(col("internet_type"))) == "DSL", "DSL")
        .when(trim(upper(col("internet_type"))) == "FIBER OPTIC", "Fiber optic")
        .when(trim(upper(col("internet_type"))).contains("FIBER"), "Fiber optic")
        .otherwise("None").alias("internet_service"),
    
    when(trim(upper(col("online_security"))) == "YES", "Yes")
        .when(trim(upper(col("online_security"))) == "NO", "No")
        .when(trim(upper(col("internet_service"))) == "NO", "Not applicable")
        .otherwise("Not applicable").alias("online_security"),
    
    when(trim(upper(col("online_backup"))) == "YES", "Yes")
        .when(trim(upper(col("online_backup"))) == "NO", "No")
        .when(trim(upper(col("internet_service"))) == "NO", "Not applicable")
        .otherwise("Not applicable").alias("online_backup"),
    
    when(trim(upper(col("device_protection_plan"))) == "YES", "Yes")
        .when(trim(upper(col("device_protection_plan"))) == "NO", "No")
        .when(trim(upper(col("internet_service"))) == "NO", "Not applicable")
        .otherwise("Not applicable").alias("device_protection"),
    
    when(trim(upper(col("premium_tech_support"))) == "YES", "Yes")
        .when(trim(upper(col("premium_tech_support"))) == "NO", "No")
        .when(trim(upper(col("internet_service"))) == "NO", "Not applicable")
        .otherwise("Not applicable").alias("tech_support"),
    
    when(trim(upper(col("streaming_tv"))) == "YES", "Yes")
        .when(trim(upper(col("streaming_tv"))) == "NO", "No")
        .when(trim(upper(col("internet_service"))) == "NO", "Not applicable")
        .otherwise("Not applicable").alias("streaming_tv"),
    
    when(trim(upper(col("streaming_movies"))) == "YES", "Yes")
        .when(trim(upper(col("streaming_movies"))) == "NO", "No")
        .when(trim(upper(col("internet_service"))) == "NO", "Not applicable")
        .otherwise("Not applicable").alias("streaming_movies"),
    
    when(trim(upper(col("streaming_music"))) == "YES", "Yes")
        .when(trim(upper(col("streaming_music"))) == "NO", "No")
        .when(trim(upper(col("internet_service"))) == "NO", "Not applicable")
        .otherwise("Not applicable").alias("streaming_music"),
    
    when(trim(upper(col("unlimited_data"))) == "YES", "Yes")
        .when(trim(upper(col("unlimited_data"))) == "NO", "No")
        .when(trim(upper(col("internet_service"))) == "NO", "Not applicable")
        .otherwise("Not applicable").alias("unlimited_data"),
    
    when(trim(upper(col("contract"))).contains("MONTH"), "Month-to-month")
        .when(trim(upper(col("contract"))).contains("ONE"), "One year")
        .when(trim(upper(col("contract"))).contains("TWO"), "Two year")
        .otherwise("Month-to-month").alias("contract"),
    
    when(trim(upper(col("payment_method"))).contains("BANK"), "Bank transfer")
        .when(trim(upper(col("payment_method"))).contains("CREDIT"), "Credit card")
        .when(trim(upper(col("payment_method"))).contains("ELECTRONIC"), "Electronic check")
        .when(trim(upper(col("payment_method"))).contains("MAIL"), "Mailed check")
        .otherwise("Unknown").alias("payment_method")
)

# ============================================================================
# Get DISTINCT Combinations + Derive Attributes
# ============================================================================

distinct_packages = services_clean.distinct()

distinct_count = distinct_packages.count()
print(f"Found {distinct_count:,} unique service packages (from {initial_count:,} customer records)")
print(f"Deduplication ratio: {initial_count / distinct_count:.1f}x")

# Derive attributes
dim_service_package = distinct_packages.withColumn(
    "total_services",
    (
        when(col("online_security") == "Yes", 1).otherwise(0) +
        when(col("online_backup") == "Yes", 1).otherwise(0) +
        when(col("device_protection") == "Yes", 1).otherwise(0) +
        when(col("tech_support") == "Yes", 1).otherwise(0) +
        when(col("streaming_tv") == "Yes", 1).otherwise(0) +
        when(col("streaming_movies") == "Yes", 1).otherwise(0) +
        when(col("streaming_music") == "Yes", 1).otherwise(0) +
        when(col("unlimited_data") == "Yes", 1).otherwise(0)
    ).cast("int")
).withColumn(
    "service_tier",
    when(col("total_services") == 0, "Basic")
        .when(col("total_services").between(1, 2), "Standard")
        .otherwise("Premium")
).withColumn(
    "bundle_type",
    when(
        (col("phone_service") == "Yes") & (col("internet_service").isin("DSL", "Fiber optic")),
        "Double play"  # Phone + Internet
    ).when(
        (col("phone_service") == "Yes") & (col("internet_service") == "None"),
        "Single service"  # Phone only
    ).when(
        (col("phone_service") == "No") & (col("internet_service").isin("DSL", "Fiber optic")),
        "Single service"  # Internet only
    ).otherwise("No service")
)

# ============================================================================
# Generate Surrogate Key
# ============================================================================

natural_key_cols = [
    "phone_service",
    "multiple_lines",
    "internet_service",
    "online_security",
    "online_backup",
    "device_protection",
    "tech_support",
    "streaming_tv",
    "streaming_movies",
    "streaming_music",
    "unlimited_data",
    "contract",
    "payment_method"
]

# Single, unique string from all columns
dim_service_package = dim_service_package.withColumn(
    "service_package_key",
    xxhash64(concat_ws("||", *natural_key_cols))
)

# Reorder columns for the final table
final_columns = ["service_package_key"] + natural_key_cols + [
    "total_services",
    "service_tier",
    "bundle_type"
]

dim_service_package = dim_service_package.select(final_columns)

print(f"Generated {dim_service_package.count():,} surrogate keys")

# ============================================================================
# Write to Silver Layer
# ============================================================================

dim_service_package.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("silver_telco.dim_service_package")

final_count = spark.table("silver_telco.dim_service_package").count()
print(f"Successfully loaded {final_count:,} rows to silver_telco.dim_service_package")

spark.stop()
