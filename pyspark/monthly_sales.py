from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils import utils

config = utils.config_details()

spark = SparkSession.builder.appName("Test").getOrCreate()
spark.conf.set('temporaryGcsBucket', config['temp_bkt'])


print("job started")
orders_tbl = config['storage_bkt_path']['orders_tbl']
customers_tbl = config['storage_bkt_path']['customers_tbl']
order_items_tbl = config['storage_bkt_path']['order_items_tbl']

df_orders = utils.create_dataframe(orders_tbl)
df_customers = utils.create_dataframe(customers_tbl,['customer_id','customer_segment'])
df_order_items = utils.create_dataframe(order_items_tbl,['order_id','product_id','quantity','price_per_unit'])

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