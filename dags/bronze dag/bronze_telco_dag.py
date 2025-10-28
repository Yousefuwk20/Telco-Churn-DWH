"""
===============================================================================
Bronze Layer Daily ETL Pipeline
===============================================================================
Pipeline Flow:
    1. Load CSV → Parquet
    2. Validate data quality
    3. Log completion

Schedule: Daily at midnight
Note: Tables must be created first.
===============================================================================
"""

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='bronze_telco_daily',
    default_args=default_args,
    description='Daily Bronze layer ETL - Load CSV to Parquet',
    schedule_interval='@daily',
    start_date=datetime(2025, 10, 28),
    catchup=False,
    tags=['bronze', 'telco', 'daily'],
)


def bronze_pipeline():
    """Daily Bronze layer ETL pipeline"""
    
    load_data = SparkSubmitOperator(
        task_id='load_bronze_data',
        application='/root/airflow/dags/scripts/bronze/load.py',
        conn_id='spark_local',
        conf={
            'spark.sql.warehouse.dir': '/user/hive/warehouse',
            'hive.metastore.uris': 'thrift://hive-metastore:9083',
        },
        executor_memory='2g',
        driver_memory='1g',
        name='bronze_load_job',
        verbose=True,
    )
    
    validate_data = SparkSubmitOperator(
        task_id='validate_bronze_data',
        application='/root/airflow/dags/scripts/bronze/validate.py',
        conn_id='spark_local',
        conf={
            'spark.sql.warehouse.dir': '/user/hive/warehouse',
            'hive.metastore.uris': 'thrift://hive-metastore:9083',
        },
        executor_memory='1g',
        driver_memory='1g',
        name='bronze_validation_job',
        verbose=True,
    )
    
    @task
    def log_success():
        """Log pipeline completion"""
        return {'status': 'success', 'timestamp': datetime.now().isoformat()}
    
    # Pipeline
    load_data >> validate_data >> log_success()


dag = bronze_pipeline()
