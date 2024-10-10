from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from utils import utils

config = utils.config_details()

spark = SparkSession.builder.appName("Test").getOrCreate()
spark.conf.set('temporaryGcsBucket', config['temp_bkt'])

order_summary_tbl = config['storage_bkt_path']['order_summary_tbl']
customer_transactions_tbl = config['storage_bkt_path']['customer_transactions_tbl']
customers_tbl = config['storage_bkt_path']['customers_tbl']


df_order_summary = utils.create_dataframe(order_summary_tbl,file_type='parquet')
df_customer_transactions = utils.create_dataframe(customer_transactions_tbl,file_type='parquet')
df_customers = utils.create_dataframe(customers_tbl,['customer_id','customer_segment','region'])

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