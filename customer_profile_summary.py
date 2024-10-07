from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("Test").getOrCreate()

def create_dataframe(url,cols="*",file_type='csv'):
    if file_type=='csv':
        df = spark.read.csv(url,header=True).select(cols)
    if file_type=='parquet':
        df = spark.read.parquet(url).select(cols)
    return df
    
spark.conf.set('temporaryGcsBucket', 'gs://temp-9283')

order_summary_tbl = 'gs://sales_analysis_curated_bkt/order_summary/'
customer_transactions_tbl = 'gs://sales_analysis_curated_bkt/customer_transactions/'
customers_tbl = 'gs://sales_data_9283/synthetic_sales_data/customers.csv'


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

df_final.write.mode('overwrite').parquet("gs://sales_analysis_agg_bkt/customer_profile_summary")
df_final.write.format('bigquery') \
  .option('table', 'ecommerce_analytics.customer_profile_summary') \
.mode('overwrite')\
  .save()