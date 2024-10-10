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

df_grouped = df_monthly_sales.groupBy('order_month').agg(sum('total_sales_amount').alias('monthly_sales'),
                                           countDistinct('customer_id').alias('unique_customers'),
                                           countDistinct('total_orders').alias('total_orders'),
                                           sum('total_quantity').alias('total_quantity_sold'),
                                           avg('total_sales_amount').alias('avg_order_value'))

df_grouped.write.mode('overwrite').parquet(config['storage_bkt_path']['monthly_sales_tbl'])

df_grouped.write.format('bigquery') \
  .option('table', config['bigquery_tables']['monthly_performance']) \
.mode('overwrite')\
  .save()