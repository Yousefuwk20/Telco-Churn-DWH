"""
===============================================================================
Silver Layer - dim_customer Transformation
===============================================================================
Purpose:
    Creates customer dimension with stable demographic attributes

Source Tables:
    - bronze_telco.demographics

Target Table:
    - silver_telco.dim_customer

Grain:
    One row per customer

Transformations:
    - Standardize gender values
    - Create age groups
    - Standardize marital status
    - Calculate family size
    - Generate surrogate keys

Usage:
    spark-submit dim_customer.py
===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, trim, upper, initcap, row_number
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Silver - dim_customer") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")

# =============================================================================
# Read Bronze Tables
# =============================================================================

demographics = spark.table("bronze_telco.demographics")
location = spark.table("bronze_telco.location")

print(f"Demographics: {demographics.count()} rows")
print(f"Location: {location.count()} rows")

# =============================================================================
# Clean Demographics Table
# =============================================================================

demographics_clean = demographics \
    .withColumn('customer_id', trim(col('customer_id'))) \
    .withColumn('age', col('age').cast('int')) \
    .withColumn('gender_raw', trim(upper(col('gender')))) \
    .withColumn('married_raw', trim(initcap(col('married')))) \
    .withColumn('number_of_dependents', col('number_of_dependents').cast('int'))

# Standardize Gender (Male/Female/Unknown)
demographics_clean = demographics_clean.withColumn(
    'gender',
    when(col('gender_raw').isin(['M', 'MALE', 'MAN']), 'Male')
    .when(col('gender_raw').isin(['F', 'FEMALE', 'WOMAN']), 'Female')
    .otherwise('n/a')
)

# Standardize Marital Status
demographics_clean = demographics_clean.withColumn(
    'marital_status',
    when(col('married_raw').isin(['Yes', 'Y', 'Married']), 'Married')
    .when(col('married_raw').isin(['No', 'N', 'Single']), 'Single')
    .otherwise('n/a')
)

# Create Age Groups
demographics_clean = demographics_clean.withColumn(
    'age_group',
    when(col('age') < 25, '18-24') #779
    .when((col('age') >= 25) & (col('age') < 35), '25-34') #1110
    .when((col('age') >= 35) & (col('age') < 45), '35-44') #1175
    .when((col('age') >= 45) & (col('age') < 55), '45-54') #1158
    .when((col('age') >= 55) & (col('age') < 65), '55-64') #1147
    .when(col('age') >= 65, '65+') #1142
)

# Calculate Family Size
demographics_clean = demographics_clean.withColumn(
    'family_size',
    when(col('marital_status') == 'Married', 
         col('number_of_dependents') + 2)  # Customer + Wife/Husband + Dependents
    .otherwise(col('number_of_dependents') + 1)  # Customer + Dependents
)

print(f"Cleaned demographics: {demographics_clean.count()} rows")


# =============================================================================
# Generate Surrogate Key
# =============================================================================
window_spec = Window.orderBy('customer_id')

dim_customer = demographics_clean.withColumn(
    'customer_key',
    row_number().over(window_spec)
)

# =============================================================================
# Select Final Columns
# =============================================================================

dim_customer_final = dim_customer.select(
    'customer_key',
    'customer_id',
    'age',
    'age_group',
    'gender',
    'marital_status',
    'number_of_dependents',
    'family_size'
)

# =============================================================================
# Write to Silver Layer
# =============================================================================

dim_customer_final.write \
    .mode('overwrite') \
    .format('parquet') \
    .saveAsTable('silver_telco.dim_customer')

final_count = spark.table('silver_telco.dim_customer').count()
print(f"Written to silver_telco.dim_customer: {final_count} rows")


spark.stop()

print("dim_customer transformation completed successfully!")
