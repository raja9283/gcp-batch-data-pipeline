from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Test").getOrCreate()

def create_dataframe(url,cols="*"):
    df = spark.read.csv(url,header=True).select(cols)
    return df

print("job started")
orders_tbl = 'gs://sales_data_9283/synthetic_sales_data/orders.csv'
customers_tbl = 'gs://sales_data_9283/synthetic_sales_data/customers.csv'
order_items_tbl = 'gs://sales_data_9283/synthetic_sales_data/order_items.csv'

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
df_grouped.write.mode('overwrite').parquet('gs://sales_analysis_curated_bkt/monthly_sales')