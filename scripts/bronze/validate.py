"""
Bronze Layer Validation Script
Validates row counts in Bronze tables
"""

from pyspark.sql import SparkSession
import sys

EXPECTED_COUNTS = {
    'demographics': 7000,
    'location': 7000,
    'population': 1600,
    'services': 7000,
    'status': 7000,
}

spark = SparkSession.builder \
    .appName("Bronze Validation") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

failed = []

for table, min_count in EXPECTED_COUNTS.items():
    query = f"SELECT COUNT(*) as cnt FROM bronze_telco.{table}"
    result = spark.sql(query).collect()
    actual = result[0]['cnt']
    
    if actual < min_count:
        failed.append(f"{table}: Expected >={min_count}, got {actual}")
        print(f"✗ {table}: {actual} rows (Expected >={min_count})")
    else:
        print(f"✓ {table}: {actual} rows")

spark.stop()

if failed:
    print("\nValidation failed:")
    for error in failed:
        print(f"  - {error}")
    sys.exit(1)
else:
    print("\nAll validations passed!")
    sys.exit(0)
