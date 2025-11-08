"""
===============================================================================
Silver Layer - dim_location Transformation
===============================================================================
Purpose:
    Creates location dimension with SCD Type 1 (updates in place)
    Combines location and population data with standardization

Source Tables:
    - bronze_telco.location
    - bronze_telco.population

Target Table:
    - silver_telco.dim_location

Grain:
    One row per unique zip code

Transformations:
    - Standardize city names (title case)
    - Format zip codes (5 digits)
    - Round coordinates to 4 decimal places
    - Validate coordinate ranges
    - Calculate population density category
    - Generate surrogate keys

Usage:
    spark-submit dim_location.py
===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, trim, initcap, lpad,
    when, row_number
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Silver - dim_location") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")

# =============================================================================
# Read Bronze Tables
# =============================================================================

location_df = spark.table("bronze_telco.location")
population_df = spark.table("bronze_telco.population")

print(f"bronze_telco.location: {location_df.count()} rows")
print(f"bronze_telco.population: {population_df.count()} rows")

# =============================================================================
# Data Cleaning - Location Table
# =============================================================================
    
location_clean = location_df \
    .dropDuplicates(['zip_code']) \
    .select(
        col('zip_code').alias('zip_code_raw'),
        col('city').alias('city_raw'),
        col('latitude').alias('latitude_raw'),
        col('longitude').alias('longitude_raw')
    )

# Standardize city names (title case)
location_clean = location_clean.withColumn(
    'city',
    initcap(trim(col('city_raw')))
)

# Format zip codes
location_clean = location_clean.withColumn(
    'zip_code',
    lpad(trim(col('zip_code_raw')), 5, '0')
)

# Round coordinates to 4 decimal places
location_clean = location_clean.withColumn(
    'latitude',
    F.round(col('latitude_raw').cast('double'), 4)
).withColumn(
    'longitude',
    F.round(col('longitude_raw').cast('double'), 4)
)


# Validate coordinate ranges
location_clean = location_clean \
    .filter((col('latitude') >= -90) & (col('latitude') <= 90)) \
    .filter((col('longitude') >= -180) & (col('longitude') <= 180))

print(f"Location data cleaned: {location_clean.count()} rows")

# =============================================================================
# Data Cleaning - Population Table
# =============================================================================

population_clean = population_df \
    .dropDuplicates(['zip_code']) \
    .select(
        col('zip_code'),
        col('population')
    )

population_clean = population_clean.withColumn(
    'zip_code',
    lpad(trim(col('zip_code')), 5, '0')
)

# Clean population - remove non-numeric characters
population_clean = population_clean.withColumn(
    'population',
    F.regexp_replace(col('population'), '[^0-9.]', '')
)

# Cast to integer and handle missing populations with 0
population_clean = population_clean.withColumn(
    'population',
    when(col('population') == '', 0)
    .otherwise(col('population').cast('double').cast('int'))
)

print(f"Population data cleaned: {population_clean.count()} rows")

# =============================================================================
# Join Location and Population Tables
# =============================================================================

dim_location = location_clean.join(
    population_clean,
    on='zip_code',
    how='left'
)

print(f"Joined data: {dim_location.count()} rows")

# =============================================================================
# Derive Population Density Category
# =============================================================================

dim_location = dim_location.withColumn(
    'population_density',
    when(col('population') >= 75000, 'Urban')
    .when(col('population') >= 20000, 'Suburban')
    .otherwise('Rural')
)

# =============================================================================
# Generate Surrogate Key
# =============================================================================

window_spec = Window.orderBy('zip_code')
dim_location = dim_location.withColumn(
    'location_key',
    row_number().over(window_spec)
)

# =============================================================================
# Select Final Columns
# =============================================================================
dim_location_final = dim_location.select(
    'location_key',
    'city',
    'zip_code',
    'latitude',
    'longitude',
    'population',
    'population_density'
)

# =============================================================================
# Write to Silver Layer
# =============================================================================

dim_location_final.write \
    .mode('overwrite') \
    .format('parquet') \
    .saveAsTable('silver_telco.dim_location')

count = spark.table('silver_telco.dim_location').count()
print(f"Written to silver_telco.dim_location: {count} rows")


# Check for nulls in key columns
null_check = spark.sql("""
    SELECT 
        SUM(CASE WHEN location_key IS NULL THEN 1 ELSE 0 END) as null_keys,
        SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) as null_cities,
        SUM(CASE WHEN zip_code IS NULL THEN 1 ELSE 0 END) as null_zips,
        SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END) as null_lat,
        SUM(CASE WHEN longitude IS NULL THEN 1 ELSE 0 END) as null_long
    FROM silver_telco.dim_location
""").collect()[0]

spark.stop()

print("\ndim_location transformation completed successfully!")
