"""
===============================================================================
Silver Layer - dim_promotion Transformation
===============================================================================
Purpose:
    Transforms bronze services data into a conformed promotion dimension.
    This is a MINI-DIMENSION capturing promotional offer information.
    
Grain:
    One row per UNIQUE COMBINATION of promotion attributes.
    
Key Concept:
    Mini-dimensions are used for low-cardinality, time-variant attributes
    that would otherwise bloat the main dimension. In this case, we track:
    - Promotional offers received by customers
    - Whether customer referred a friend
    
    These attributes can change over time, so they're tracked separately
    from the customer dimension.
    
Source:
    - bronze_telco.services
    
Target:
    - silver_telco.dim_promotion

Transformations:
    1. Data Cleaning & Standardization:
       - Standardize offer values (Offer A/B/C/D/E/None)
       - Standardize referred_a_friend (Yes/No)
    
    2. Get DISTINCT combinations of promotion attributes
    
    3. Generate surrogate key using deterministic hash
    
Usage:
    spark-submit dim_promotion.py
===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, upper, xxhash64, concat_ws
)

spark = SparkSession.builder \
    .appName("Load dim_promotion") \
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

promotion_clean = bronze_services.select(
    when(trim(upper(col("offer"))).contains("OFFER A"), "Offer A")
        .when(trim(upper(col("offer"))).contains("OFFER B"), "Offer B")
        .when(trim(upper(col("offer"))).contains("OFFER C"), "Offer C")
        .when(trim(upper(col("offer"))).contains("OFFER D"), "Offer D")
        .when(trim(upper(col("offer"))).contains("OFFER E"), "Offer E")
        .when(trim(col("offer")).isNull(), "None")
        .when(trim(col("offer")) == "", "None")
        .otherwise("None").alias("offer"),
    
    when(trim(upper(col("referred_a_friend"))) == "YES", "Yes")
        .when(trim(upper(col("referred_a_friend"))) == "NO", "No")
        .otherwise("No").alias("referred_a_friend")
)


# ============================================================================
# Get DISTINCT Combinations
# ============================================================================

distinct_promotions = promotion_clean.distinct()

distinct_count = distinct_promotions.count()
print(f"Found {distinct_count:,} unique promotion combinations (from {initial_count:,} customer records)")

# ============================================================================
# Generate Surrogate Key (Deterministic Hash)
# ============================================================================

dim_promotion = distinct_promotions.withColumn(
    "promotion_key",
    xxhash64(concat_ws("||", col("offer"), col("referred_a_friend")))
)

# Reorder columns
dim_promotion = dim_promotion.select(
    "promotion_key",
    "offer",
    "referred_a_friend"
)

print(f"Generated {dim_promotion.count():,} surrogate keys")


# ============================================================================
# Write to Silver Layer
# ============================================================================

dim_promotion.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("silver_telco.dim_promotion")

final_count = spark.table("silver_telco.dim_promotion").count()
print(f"Successfully loaded {final_count:,} rows to silver_telco.dim_promotion")

spark.stop()
