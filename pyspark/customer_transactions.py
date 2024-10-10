from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
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


orders_tbl = config['storage_bkt_path']['orders_tbl']
order_items_tbl = config['storage_bkt_path']['order_items_tbl']

df_orders = create_dataframe(orders_tbl)
df_order_items = create_dataframe(order_items_tbl,['order_id','product_id','quantity','price_per_unit'])

df_orders_filterd = df_orders.filter(df_orders.order_status.isin('completed','returned'))

df_orders_joined = df_orders_filterd.join(df_order_items,on=['order_id'],how='inner')
df_orders_joined = df_orders_joined.withColumn('sales_amount',round(df_orders_joined.quantity * df_orders_joined.price_per_unit,2))\
                    .withColumn('temp_col',when(df_orders_joined.order_status=='returned',1).otherwise(0))

df_grouped = df_orders_joined.groupBy('customer_id').agg(count("order_id").alias("total_orders"),
                                                        sum('quantity').alias('total_quantity_purchased'),
                                                        sum('sales_amount').alias('total_sales_value'),
                                                        countDistinct('product_id').alias('total_distinct_products'),
                                                        min('order_date').alias('first_purchase_date'),
                                                        max('order_date').alias('last_purchase_date'),
                                                        sum('temp_col').alias('returned_orders'))

df_final = df_grouped.withColumn('avg_order_value',round(df_grouped.total_sales_value/df_grouped.total_orders,2))

df_final.write.mode("overwrite").parquet(config['storage_bkt_path']['customer_transactions_tbl'])