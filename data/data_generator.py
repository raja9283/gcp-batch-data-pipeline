import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

# Initialize Faker
fake = Faker()

# Set number of records for each table
NUM_CUSTOMERS = 10000      # Number of customers
NUM_PRODUCTS = 500         # Number of products
NUM_ORDERS = 500000        # Number of orders
NUM_ORDER_ITEMS = 2000000  # Number of order items
NUM_TRANSACTIONS = 500000  # Number of transactions (1 per order)

# Output directory
OUTPUT_DIR = "synthetic_sales_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Helper Functions
def random_date(start, end):
    """Generate a random datetime between `start` and `end`"""
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

# 1. Generate `customers` table
def generate_customers(num_customers=NUM_CUSTOMERS):
    customers = []
    customer_segments = ['Regular', 'Premium', 'VIP', 'Loyal', 'Occasional']
    regions = ['North America', 'Europe', 'Asia', 'South America', 'Australia', 'Africa', 'Middle East', 'Southeast Asia']
    
    for _ in range(num_customers):
        customer_id = fake.uuid4()
        customer_name = fake.name()
        signup_date = random_date(datetime(2018, 1, 1), datetime(2024, 10, 1))
        region = random.choice(regions)
        segment = random.choice(customer_segments)
        customers.append([customer_id, customer_name, signup_date, region, segment])

    customers_df = pd.DataFrame(customers, columns=[
        'customer_id', 'customer_name', 'signup_date', 'region', 'customer_segment'])
    customers_df.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
    print(f"Generated {num_customers} customers.")
    return customers_df

# 2. Generate `products` table
def generate_products(num_products=NUM_PRODUCTS):
    categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys', 'Sports', 'Beauty', 'Automotive']
    sub_categories = {
        'Electronics': ['Mobile Phones', 'Laptops', 'Accessories', 'Cameras', 'TVs'],
        'Clothing': ['Men', 'Women', 'Kids', 'Accessories', 'Footwear'],
        'Home & Kitchen': ['Appliances', 'Furniture', 'Kitchenware', 'Bedding', 'Decor'],
        'Books': ['Fiction', 'Non-Fiction', 'Comics', 'Textbooks', 'E-books'],
        'Toys': ['Action Figures', 'Dolls', 'Educational', 'Board Games', 'Outdoor Toys'],
        'Sports': ['Fitness Equipment', 'Outdoor Gear', 'Team Sports', 'Cycling', 'Running'],
        'Beauty': ['Skincare', 'Makeup', 'Haircare', 'Fragrances', 'Nail Care'],
        'Automotive': ['Car Accessories', 'Motorcycle Gear', 'Tools', 'Electronics', 'Parts'],
    }
    
    products = []
    for _ in range(num_products):
        product_id = fake.uuid4()
        category = random.choices(categories, weights=[0.3, 0.2, 0.2, 0.1, 0.1, 0.05, 0.025, 0.025])[0]  # Uneven distribution
        sub_category = random.choice(sub_categories[category])
        brand = fake.company()
        products.append([product_id, category, sub_category, brand])

    products_df = pd.DataFrame(products, columns=[
        'product_id', 'category', 'sub_category', 'brand'])
    products_df.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)
    print(f"Generated {num_products} products.")
    return products_df

# 3. Generate `orders` table
def generate_orders(num_orders=NUM_ORDERS, customers=None):
    order_statuses = ['completed', 'shipped', 'returned', 'cancelled']
    payment_methods = ['credit_card', 'paypal', 'cash', 'bank_transfer', 'gift_card']
    
    orders = []
    for _ in range(num_orders):
        order_id = fake.uuid4()
        customer_id = random.choice(customers['customer_id'])
        order_date = random_date(datetime(2019, 1, 1), datetime(2024, 12, 31))
        order_status = random.choices(order_statuses, weights=[0.7, 0.2, 0.05, 0.05])[0]
        total_amount = round(random.uniform(2, 500), 2)
        discount_amount = round(total_amount * random.uniform(0, 0.15), 2)  # Up to 15% discount
        payment_method = random.choice(payment_methods)
        orders.append([order_id, customer_id, order_date, order_status, total_amount, discount_amount, payment_method])

    orders_df = pd.DataFrame(orders, columns=[
        'order_id', 'customer_id', 'order_date', 'order_status', 'total_amount', 'discount_amount', 'payment_method'])
    orders_df.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False)
    print(f"Generated {num_orders} orders.")
    return orders_df

# 4. Generate `order_items` table
def generate_order_items(num_order_items=NUM_ORDER_ITEMS, orders=None, products=None):
    order_items = []
    
    for _ in range(num_order_items):
        order_item_id = fake.uuid4()
        order_id = random.choice(orders['order_id'])
        product_id = random.choice(products['product_id'])
        quantity = random.randint(1, 5)
        price_per_unit = round(random.uniform(1, 150), 2)
        order_items.append([order_item_id, order_id, product_id, quantity, price_per_unit])

    order_items_df = pd.DataFrame(order_items, columns=[
        'order_item_id', 'order_id', 'product_id', 'quantity', 'price_per_unit'])
    order_items_df.to_csv(f"{OUTPUT_DIR}/order_items.csv", index=False)
    print(f"Generated {num_order_items} order items.")
    return order_items_df

# 5. Generate `transactions` table
def generate_transactions(num_transactions=NUM_TRANSACTIONS, orders=None):
    transaction_statuses = ['success', 'failed']
    
    transactions = []
    for _ in range(num_transactions):
        transaction_id = fake.uuid4()
        order_id = random.choice(orders['order_id'])
        transaction_date = random_date(datetime(2019, 1, 1), datetime(2024, 12, 31))
        transaction_amount = round(random.uniform(2, 500), 2)
        transaction_status = random.choices(transaction_statuses, weights=[0.95, 0.05])[0]
        transactions.append([transaction_id, order_id, transaction_date, transaction_amount, transaction_status])

    transactions_df = pd.DataFrame(transactions, columns=[
        'transaction_id', 'order_id', 'transaction_date', 'transaction_amount', 'transaction_status'])
    transactions_df.to_csv(f"{OUTPUT_DIR}/transactions.csv", index=False)
    print(f"Generated {num_transactions} transactions.")
    return transactions_df

# Run the data generation
customers_df = generate_customers()
products_df = generate_products()
orders_df = generate_orders(customers=customers_df)
order_items_df = generate_order_items(orders=orders_df, products=products_df)
transactions_df = generate_transactions(orders=orders_df)

print(f"Synthetic data generation completed. Files are saved in {OUTPUT_DIR}.")
