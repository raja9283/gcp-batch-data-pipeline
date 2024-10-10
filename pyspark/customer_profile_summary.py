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
spark.conf.set('temporaryGcsBucket', config['temp_bkt'])

order_summary_tbl = config['storage_bkt_path']['order_summary_tbl']
customer_transactions_tbl = config['storage_bkt_path']['customer_transactions_tbl']
customers_tbl = config['storage_bkt_path']['customers_tbl']


df_order_summary = create_dataframe(order_summary_tbl,file_type='parquet')
df_customer_transactions = create_dataframe(customer_transactions_tbl,file_type='parquet')
df_customers = create_dataframe(customers_tbl,['customer_id','customer_segment','region'])

df_customer_categories = df_order_summary.filter(df_order_summary.order_status=='completed')\
                            .groupBy('customer_id','category')\
                            .agg(sum('total_quantity').alias('total_quantity'))\
                            .withColumn('rank',row_number().over(Window.partitionBy('customer_id').orderBy(desc("total_quantity"))))
                            
df_customer_categories = df_customer_categories.filter(df_customer_categories.rank==1).select('customer_id','category')

df_join = df_customer_transactions.join(df_customers,on=['customer_id'],how='left')\
.join(df_customer_categories,on=['customer_id'],how='left')

df_final = df_join.withColumn('average_order_value',round(df_join.avg_order_value,2))\
            .withColumn('customer_lifetime_days',datediff(df_join.last_purchase_date,df_join.first_purchase_date))\
            .withColumn('return_rate',when(df_join.total_orders!=0,round((df_join.returned_orders/df_join.total_orders)*100,2)).otherwise(0))\
            .withColumn('total_revenue',round(df_join.total_sales_value,2))\
            .withColumn('first_purchase_date',to_timestamp(col("first_purchase_date"), "yyyy-MM-dd HH:mm:ss"))\
            .withColumn('recent_purchase_date',to_timestamp(col("last_purchase_date"), "yyyy-MM-dd HH:mm:ss"))\
            .withColumnRenamed('category','most_purchased_category').drop('avg_order_value','total_sales_value','last_purchase_date')

df_final.write.mode('overwrite').parquet(config['storage_bkt_path']['customer_profile_summary_tbl'])
df_final.write.format('bigquery') \
  .option('table', config['bigquery_tables']['customer_profile_summary']) \
.mode('overwrite')\
  .save()