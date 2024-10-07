from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'sales_analysis',
    default_args=default_args,
    description='A DAG to execute Dataproc tasks for sales analytics',
    schedule_interval='0 6,18 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Define the cluster name and region
    CLUSTER_NAME = 'cluster-9fa3'
    REGION = 'us-central1'
    PROJECT_ID = 'rising-field-430109-g4'
    ORDER_SUMMARY_URL = 'gs://stage_bkt9283/code/order_summary.py'
    CUSTOMER_TRANSACTIONS_URL = 'gs://stage_bkt9283/code/customer_transactions.py'
    CUSTOMER_PROFILE_SUMMARY_URL = 'gs://stage_bkt9283/code/customer_profile_summary.py'
    MONTHLY_SALES_URL = 'gs://stage_bkt9283/code/monthly_sales.py'
    CUSTOMER_RETENTION_URL = 'gs://stage_bkt9283/code/customer_retention.py'
    MONTHLY_PERFORMANCE_URL = 'gs://stage_bkt9283/code/monthly_performance.py'
    
    # Define the first Dataproc job
    order_summary = DataprocSubmitJobOperator(
        task_id='order_summary',
        job={
            'reference': {'project_id': PROJECT_ID,'job_id': f'order_summary_{datetime.now().strftime("%Y%m%d%H%M%S")}'},
            'placement': {'cluster_name': CLUSTER_NAME},
            'pyspark_job': {'main_python_file_uri': ORDER_SUMMARY_URL},
        },
        region=REGION,
    )

    # Define the second Dataproc
    customer_transactions = DataprocSubmitJobOperator(
        task_id='customer_transactions',
        job={
            'reference': {'project_id': PROJECT_ID,'job_id': f'customer_transactions_{datetime.now().strftime("%Y%m%d%H%M%S")}'},
            'placement': {'cluster_name': CLUSTER_NAME},
            'pyspark_job': {'main_python_file_uri': CUSTOMER_TRANSACTIONS_URL},
        },
        region=REGION,
    )

    # Define the third Dataproc job
    customer_profile_summary = DataprocSubmitJobOperator(
        task_id='customer_profile_summary',
        job={
            'reference': {'project_id': PROJECT_ID,'job_id': f'customer_profile_summary_{datetime.now().strftime("%Y%m%d%H%M%S")}'},
            'placement': {'cluster_name': CLUSTER_NAME},
            'pyspark_job': {'main_python_file_uri': CUSTOMER_PROFILE_SUMMARY_URL},
        },
        region=REGION,
    )
    # Define the forth Dataproc job
    monthly_sales = DataprocSubmitJobOperator(
        task_id='monthly_sales',
        job={
            'reference': {'project_id': PROJECT_ID,'job_id': f'monthly_sales_{datetime.now().strftime("%Y%m%d%H%M%S")}'},
            'placement': {'cluster_name': CLUSTER_NAME},
            'pyspark_job': {'main_python_file_uri': MONTHLY_SALES_URL},
        },
        region=REGION,
    )

    # Define the fifth Dataproc job 
    customer_retention = DataprocSubmitJobOperator(
        task_id='customer_retention',
        job={
            'reference': {'project_id': PROJECT_ID,'job_id': f'customer_retention_{datetime.now().strftime("%Y%m%d%H%M%S")}'},
            'placement': {'cluster_name': CLUSTER_NAME},
            'pyspark_job': {'main_python_file_uri': CUSTOMER_RETENTION_URL},
        },
        region=REGION,
    )

    # Define the sixth Dataproc job 
    monthly_performance = DataprocSubmitJobOperator(
        task_id='monthly_performance',
        job={
            'reference': {'project_id': PROJECT_ID,'job_id': f'monthly_performance_{datetime.now().strftime("%Y%m%d%H%M%S")}'},
            'placement': {'cluster_name': CLUSTER_NAME},
            'pyspark_job': {'main_python_file_uri': MONTHLY_PERFORMANCE_URL},
        },
        region=REGION,
    )
    order_summary >> customer_transactions >> customer_profile_summary
    monthly_sales >> [customer_retention,monthly_performance]
