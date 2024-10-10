from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from utils import utils

config = utils.config_details()

spark = SparkSession.builder.appName("Test").getOrCreate()
spark.conf.set('temporaryGcsBucket', config['temp_bkt'])


orders_tbl = config['storage_bkt_path']['orders_tbl']
order_items_tbl = config['storage_bkt_path']['order_items_tbl']

df_orders = utils.create_dataframe(orders_tbl)
df_order_items = utils.create_dataframe(order_items_tbl,['order_id','product_id','quantity','price_per_unit'])

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