"""
===============================================================================
Silver Layer - dim_quarter Transformation
===============================================================================
Purpose:
    Transforms quarterly snapshot metadata into a proper time dimension.
    
Business Rule:
    - Initial load: All 'Q3' data is assigned to year 2025
    - Future loads: Year will be determined by data governance/file metadata
    
Grain:
    One row per unique quarter
    
Source:
    - bronze_telco.status (quarter field)
    - Applied business rule for year assignment
    
Target:
    - silver_telco.dim_quarter

Logic:
    1. Extract distinct quarters from bronze status table
    2. Apply year assignment rule (2025 for current data)
    3. Derive quarter_key (YYYYQQ format)
    4. Derive hierarchical attributes (half_of_year)
    5. Load to dim_quarter (overwrite mode for idempotency)
    
Usage:
    spark-submit dim_quarter.py
===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, concat, when, regexp_extract
)

spark = SparkSession.builder \
    .appName("Load dim_quarter") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")

# ============================================================================
# Read distinct quarters from bronze status table
# ============================================================================

bronze_status = spark.table("bronze_telco.status")
distinct_quarters = bronze_status.select("quarter").distinct()
print(f"Found {distinct_quarters.count()} distinct quarter values")
distinct_quarters.show()

# ============================================================================
# Apply business rule - Assign year based on data context
# ============================================================================

BUSINESS_YEAR = 2025
quarters_with_year = distinct_quarters.withColumn("year", lit(BUSINESS_YEAR))
print(f"Assigned year {BUSINESS_YEAR} to all quarters")
quarters_with_year.show()

# ============================================================================
# Parse quarter number and derive attributes
# ============================================================================

dim_quarter = quarters_with_year.withColumn(
    "quarter_of_year",
    regexp_extract(col("quarter"), r"Q(\d)", 1).cast("int")
).withColumn(
    "quarter_key",
    (col("year") * 100 + col("quarter_of_year")).cast("int")
).withColumn(
    "quarter_name",
    concat(col("quarter"), lit("-"), col("year").cast("string"))
).withColumn(
    "half_of_year",
    when(col("quarter_of_year") <= 2, "H1").otherwise("H2")
)

# Select final columns
dim_quarter = dim_quarter.select(
    "quarter_key",
    "quarter_name",
    "year",
    "quarter_of_year",
    "half_of_year"
)

dim_quarter.show(truncate=False)

# ============================================================================
# Write to Silver Layer
# ============================================================================

dim_quarter.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("silver_telco.dim_quarter")

final_count = spark.table("silver_telco.dim_quarter").count()

spark.stop()

print(f"Successfully loaded {final_count} rows to silver_telco.dim_quarter")

