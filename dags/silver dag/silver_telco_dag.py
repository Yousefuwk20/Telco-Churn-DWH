"""
===============================================================================
Silver Layer ETL Pipeline - Conformed Dimensions
===============================================================================
Pipeline Flow:
    1. Load all 6 dimensions in parallel:
       - dim_customer
       - dim_service_package
       - dim_location
       - dim_churn_status
       - dim_quarter
       - dim_promotion
    2. Validate dimension data quality

Schedule: Daily
Dependencies: Requires bronze_telco tables to exist
===============================================================================
"""

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Shared Spark configuration
SPARK_COMMON_CONFIG = {
    'conn_id': 'spark_local',
    'conf': {
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'hive.metastore.uris': 'thrift://hive-metastore:9083',
    },
    'verbose': True,
}

DIMENSIONS = [
    {'name': 'customer', 'executor_memory': '2g', 'driver_memory': '1g'},
    {'name': 'service_package', 'executor_memory': '2g', 'driver_memory': '1g'},
    {'name': 'location', 'executor_memory': '2g', 'driver_memory': '1g'},
    {'name': 'churn_status', 'executor_memory': '1g', 'driver_memory': '1g'},
    {'name': 'quarter', 'executor_memory': '1g', 'driver_memory': '1g'},
    {'name': 'promotion', 'executor_memory': '1g', 'driver_memory': '1g'},
]

@dag(
    dag_id='silver_telco_daily',
    default_args=default_args,
    description='Daily Silver layer ETL - Load conformed dimensions',
    schedule_interval='@daily',
    start_date=datetime(2025, 11, 8),
    catchup=False,
    tags=['silver', 'telco', 'daily']
)

def silver_pipeline():
    """Daily Silver layer ETL pipeline - Conformed Dimensions"""

    dimension_tasks = []
    for dim in DIMENSIONS:
        dim_task = SparkSubmitOperator(
            task_id=f'load_dim_{dim["name"]}',
            application=f'/root/airflow/dags/scripts/silver/dim_{dim["name"]}.py',
            executor_memory=dim['executor_memory'],
            driver_memory=dim['driver_memory'],
            name=f'dim_{dim["name"]}_job',
            **SPARK_COMMON_CONFIG,
        )
        dimension_tasks.append(dim_task)
    
    @task
    def validate_dimensions():
        """
        Checks if counts are valid and fails if any dimension is empty.
        """
        from airflow.providers.apache.hive.hooks.hive import HiveCliHook
        
        hook = HiveCliHook(hive_cli_conn_id='hive_default')
        
        results = {}
        all_valid = True
        
        for dim in DIMENSIONS:
            dim_name = f"dim_{dim['name']}"
            
            query = f"SELECT COUNT(*) FROM silver_telco.{dim_name};"
            result = hook.run_cli(hql=query)
            count = int(result.strip()) if result.strip().isdigit() else 0
            results[dim_name] = count
            print(f"{dim_name}: {count:,} rows")

            if count == 0:
                print(f"ERROR: {dim_name} is empty!")
                all_valid = False
        
        if not all_valid:
            raise ValueError(
                f"Validation failed! One or more dimensions are empty. "
                f"Results: {results}"
            )
        
        print(f"\nAll dimensions validated successfully!")
        return results
    
    @task
    def log_completion(**context):
        """Lightweight logging task"""
        validation_results = context['ti'].xcom_pull(task_ids='validate_dimensions')
        return {
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
            'layer': 'silver',
            'dimension_counts': validation_results
        }

    dimension_tasks >> validate_dimensions() >> log_completion()


dag = silver_pipeline()
