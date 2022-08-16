from airflow.hooks.S3_hook import S3Hook
import os
import boto3
from dotenv import load_dotenv
import pandas as pd
import time
import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


load_dotenv('/home/nikol/internship/task8/task8/.env')

AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
LOCALSTACK_URL = os.environ.get('LOCALSTACK_S3_URL')

def upload_to_s3(bucket_name: str, subname: str) -> None:
    df = pd.read_csv('data/date.csv')
    s3 = boto3.client("s3", endpoint_url=LOCALSTACK_URL, aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY)
    for i in set(df.month):
        filename = subname+i+".csv"    
        s3.upload_file(Filename=filename, Key=filename, Bucket=bucket_name)
        time.sleep(3)

def processed_by_pyspark_csv_saver():
    #saves pyspark dataframe into csv
    df = pd.read_csv('data/date.csv')
    for i in set(df.month):
        filename = "data/"+i+".csv"
        res = process_pyspark(filename)
        filename = "data/processed_"+i+".csv"
        res.toPandas().to_csv(filename, index=False)

def process_pyspark(filename):
    # returns aggregated pyspark dataframe
    count_by_dep_name = spark.read.option("header",True).csv(filename) \
    .groupBy('departure_name').count() \
    .withColumnRenamed("count","departure_name_count") \
    .withColumnRenamed("departure_name","station_name")

    count_by_return_name = spark.read.option("header",True).csv(filename) \
    .groupBy('return_name').count() \
    .withColumnRenamed("return_name","station_name") \
    .withColumnRenamed("count","return_name_count") \

    result = count_by_dep_name \
    .join(count_by_return_name, count_by_dep_name.station_name == count_by_return_name.station_name, "inner") \
    .drop(count_by_return_name.station_name) \
        
    return result
