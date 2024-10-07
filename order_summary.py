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
products_tbl = 'gs://sales_data_9283/synthetic_sales_data/products.csv'

df_orders = create_dataframe(orders_tbl)
df_customers = create_dataframe(customers_tbl,['customer_id','customer_segment','region'])
df_order_items = create_dataframe(order_items_tbl,['order_id','product_id','quantity','price_per_unit'])
df_products = create_dataframe(products_tbl,['product_id','category','sub_category','brand'])
print("files read completed")
df_orders_filterd = df_orders.filter(df_orders.order_status.isin('completed','returned'))
print("join start")
df_orders_joined = df_orders_filterd.join(df_customers,on=["customer_id"],how='inner')\
.join(df_order_items,on=['order_id'],how='inner')\
.join(df_products,on=['product_id'],how='inner')
print("join ended")
df_orders_joined = df_orders_joined.withColumn('sales_amount',df_orders_joined.quantity * df_orders_joined.price_per_unit)

df_grouped = df_orders_joined.groupBy('order_id','customer_id','customer_segment','order_date','region','order_status','category','sub_category','brand').agg(sum("quantity").alias('total_quantity'),sum('sales_amount').alias('total_sales_amount'),sum('discount_amount').alias('total_discount_amount'),countDistinct('product_id').alias('distinct_products'))
print("writing to cloud storage.")
df_grouped.write.mode("overwrite").parquet('gs://sales_analysis_curated_bkt/order_summary')
