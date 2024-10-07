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

df_grouped = df_monthly_sales.groupBy('order_month').agg(sum('total_sales_amount').alias('monthly_sales'),
                                           countDistinct('customer_id').alias('unique_customers'),
                                           countDistinct('total_orders').alias('total_orders'),
                                           sum('total_quantity').alias('total_quantity_sold'),
                                           avg('total_sales_amount').alias('avg_order_value'))

df_grouped.write.mode('overwrite').parquet('gs://sales_analysis_agg_bkt/monthly_performance')
df_grouped.write.format('bigquery') \
  .option('table', 'ecommerce_analytics.monthly_performance') \
.mode('overwrite')\
  .save()