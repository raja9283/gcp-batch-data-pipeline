from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils import utils

config = utils.config_details()

spark = SparkSession.builder.appName("Test").getOrCreate()
spark.conf.set('temporaryGcsBucket', config['temp_bkt'])

monthly_sales_tbl = config['storage_bkt_path']['monthly_sales_tbl']

df_monthly_sales = utils.create_dataframe(monthly_sales_tbl,file_type = 'parquet')

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