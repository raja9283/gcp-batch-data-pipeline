from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Test").getOrCreate()
spark.conf.set('temporaryGcsBucket', 'gs://temp-9283')

def create_dataframe(url,cols="*",file_type='csv'):
    if file_type=='csv':
        df = spark.read.csv(url,header=True).select(cols)
    if file_type=='parquet':
        df = spark.read.parquet(url).select(cols)
    return df

monthly_sales_tbl = 'gs://sales_analysis_curated_bkt/monthly_sales'

df_monthly_sales = create_dataframe(monthly_sales_tbl,file_type = 'parquet')

df_monthly_order_counts = df_monthly_sales.groupBy('customer_id', 'order_month')\
                .agg(count('total_orders').alias('monthly_order_count'))
df_monthly_order_counts = df_monthly_order_counts.withColumn('temp_column',when(df_monthly_order_counts.monthly_order_count>1,df_monthly_order_counts.order_month).otherwise(None))

df_customer_retention = df_monthly_order_counts.groupBy('customer_id').agg(countDistinct('order_month').alias('months_active'),
                                                   min('order_month').alias('first_purchase_month'),
                                                   max('order_month').alias('last_purchase_month'),
                                                   countDistinct('temp_column').alias('repeat_purchase_months'))

df_customer_retention.write.mode('overwrite').parquet('gs://sales_analysis_agg_bkt/customer_retention')
df_customer_retention.write.format('bigquery') \
  .option('table', 'ecommerce_analytics.customer_retention') \
.mode('overwrite')\
  .save()