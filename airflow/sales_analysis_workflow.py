import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from datetime import datetime
from ..pyspark. utils import utils

utils.load_env()
config = utils.config_details()


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
    CLUSTER_NAME = os.getenv('CLUSTER_NAME')
    REGION = os.getenv('REGION')
    PROJECT_ID = os.getenv('PROJECT_ID')
    ORDER_SUMMARY_URL = config['dataflow_source']['order_summary_url']
    CUSTOMER_TRANSACTIONS_URL = config['dataflow_source']['customer_transactions_url']
    CUSTOMER_PROFILE_SUMMARY_URL = config['dataflow_source']['customer_profile_summary_url']
    MONTHLY_SALES_URL = config['dataflow_source']['monthly_sales_url']
    CUSTOMER_RETENTION_URL = config['dataflow_source']['customer_retention_url']
    MONTHLY_PERFORMANCE_URL = config['dataflow_source']['monthly_performance_url']
    
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
