from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
from google.cloud import storage
from urllib.parse import urlparse

def read_and_load_gcs_file(gcs_path):
    """Reads a .env or config.json file from a GCS path and loads its contents as environment variables."""
    # Parse the GCS path
    parsed_url = urlparse(gcs_path)
    bucket_name = parsed_url.netloc
    file_path = parsed_url.path.lstrip('/')  # Remove leading slash from path

    # Initialize the GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    # Read the contents of the file as text
    file_data = blob.download_as_text()

    # Parse JSON file and set environment variables
    config_vars = json.loads(file_data)
    
    return config_vars

    
def create_dataframe(url,cols="*",file_type='csv'):
    if file_type=='csv':
        df = spark.read.csv(url,header=True).select(cols)
    if file_type=='parquet':
        df = spark.read.parquet(url).select(cols)
    return df
config = read_and_load_gcs_file('gs://stage_bkt9283/code/config.json')

spark = SparkSession.builder.appName("Test").getOrCreate()

spark = SparkSession.builder.appName("Test").getOrCreate()
spark.conf.set('temporaryGcsBucket', config['temp_bkt'])

monthly_sales_tbl = config['storage_bkt_path']['monthly_sales_tbl']

df_monthly_sales = create_dataframe(monthly_sales_tbl,file_type = 'parquet')

df_monthly_order_counts = df_monthly_sales.groupBy('customer_id', 'order_month')\
                .agg(count('total_orders').alias('monthly_order_count'))
df_monthly_order_counts = df_monthly_order_counts.withColumn('temp_column',when(df_monthly_order_counts.monthly_order_count>1,df_monthly_order_counts.order_month).otherwise(None))

df_customer_retention = df_monthly_order_counts.groupBy('customer_id').agg(countDistinct('order_month').alias('months_active'),
                                                   min('order_month').alias('first_purchase_month'),
                                                   max('order_month').alias('last_purchase_month'),
                                                   countDistinct('temp_column').alias('repeat_purchase_months'))

df_customer_retention.write.mode('overwrite').parquet(config['storage_bkt_path']['customer_retention_tbl'])
df_customer_retention.write.format('bigquery') \
  .option('table',config['bigquery_tables']['customer_retention']) \
.mode('overwrite')\
  .save()