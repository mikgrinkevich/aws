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
AWS_REGION = os.environ.get('AWS_REGION')

def create_client(service_name: str):
    boto3_client = boto3.client(
        service_name, 
        region_name=AWS_REGION, 
        endpoint_url=LOCALSTACK_URL,
        aws_access_key_id=AWS_ACCESS_KEY, 
        aws_secret_access_key=AWS_SECRET_KEY,
        verify=False
        )
    return boto3_client

def get_list_of_months():
    # getting a list to iterate over
    df = pd.read_csv('data/database_processed.csv')
    cols = df.columns
    date_list = list(set(df.month))
    return date_list

def upload_raw_to_s3(bucket_name: str, subname: str) -> None:
    list_of_months = get_list_of_months()
    s3_client = create_client('s3')
    client_sqs = create_client('sqs')
    for i in list_of_months:
        filename = subname+i+".csv"    
        s3_client.upload_file(Filename=filename, Key=i+".csv", Bucket=bucket_name)
        time.sleep(1)
        send_message_sqs(client_sqs, i)
        receive_message_sqs(client_sqs)


def upload_pyspark_to_s3(bucket_name: str, subname: str) -> None:
    list_of_months = get_list_of_months()
    s3_client = create_client('s3')
    for i in list_of_months:
        filename = subname+i+".csv"    
        s3_client.upload_file(Filename=filename, Key="processed_"+i+".csv", Bucket=bucket_name)
        time.sleep(1)

def processed_by_pyspark_csv_saver():
    list_of_months = get_list_of_months()
    for i in list_of_months:
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


def send_message_sqs(client, year_month):
    response = client.send_message(
    QueueUrl = 'http://localhost:4566/000000000000/test',
    DelaySeconds=3,
    MessageAttributes={
        'Key': {
            'DataType': 'String',
            'StringValue': '16_05'
        }
    },
    MessageBody=(
        year_month
        )
    )
    print(response['MessageId'])


def receive_message_sqs(client):
    response = client.receive_message(
    QueueUrl='http://localhost:4566/000000000000/test',
    AttributeNames=[
        'Policy',
    ],
    MessageAttributeNames=[
        'Key',
    ],
    MaxNumberOfMessages=1,
    VisibilityTimeout=1,
    WaitTimeSeconds=1,
    ReceiveRequestAttemptId='string'
)
    print(response)

# send_message_sqs(create_client('sqs'))
# receive_message_sqs(create_client('sqs'))

