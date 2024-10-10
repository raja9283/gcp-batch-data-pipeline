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


print("job started")
orders_tbl = config['storage_bkt_path']['orders_tbl']
customers_tbl = config['storage_bkt_path']['customers_tbl']
order_items_tbl = config['storage_bkt_path']['order_items_tbl']

df_orders = create_dataframe(orders_tbl)
df_customers = create_dataframe(customers_tbl,['customer_id','customer_segment'])
df_order_items = create_dataframe(order_items_tbl,['order_id','product_id','quantity','price_per_unit'])

print("files read completed")
df_orders_filterd = df_orders.filter(df_orders.order_status =='completed')
print("join start")
df_orders_joined = df_orders_filterd.join(df_customers,on=["customer_id"],how='inner')\
.join(df_order_items,on=['order_id'],how='inner')
print("join ended")
df_orders_joined = df_orders_joined.withColumn('sales_amount',df_orders_joined.quantity * df_orders_joined.price_per_unit)\
                                    .withColumn('order_month',trunc(df_orders_joined.order_date,'month'))

df_grouped = df_orders_joined.groupBy('order_month','customer_id','customer_segment').agg(sum("quantity").alias('total_quantity'),
                                                                                          sum('sales_amount').alias('total_sales_amount'),
                                                                                          countDistinct('order_id').alias('total_orders'),
                                                                                         countDistinct('product_id').alias('distinct_products_purchased'))

print("writing to cloud storage.")
df_grouped.write.mode('overwrite').parquet(config['storage_bkt_path']['monthly_sales_tbl'])