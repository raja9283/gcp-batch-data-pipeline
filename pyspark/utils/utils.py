# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
from dotenv import load_dotenv
import os
import json

# spark = SparkSession.builder.appName("Test").getOrCreate()

def config_details():
    # Construct the path to the config JSON file
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'config.json')
    
    # Read the JSON configuration
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)
    return config
    
def create_dataframe(url,cols="*",file_type='csv'):
    if file_type=='csv':
        df = spark.read.csv(url,header=True).select(cols)
    if file_type=='parquet':
        df = spark.read.parquet(url).select(cols)
    return df

def load_env():
    # Construct the path to the .env file located two levels above
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'..', '..', '.env')
    print(env_path)
    # Load environment variables from the specified path
    load_dotenv(dotenv_path=env_path)
    