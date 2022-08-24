import json
import logging
from datetime import date, datetime
import time
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv('/home/nikol/internship/task8/task8/.env')

AWS_REGION = os.environ.get('AWS_REGION')
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
LOCALSTACK_URL = os.environ.get('LOCALSTACK_S3_URL')

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

def create_sqs():
    sqs = boto3.resource(
        'sqs', 
        region_name=AWS_REGION, 
        endpoint_url=LOCALSTACK_URL,
        aws_access_key_id=AWS_ACCESS_KEY, 
        aws_secret_access_key=AWS_SECRET_KEY,
        verify=False
        )

    queue = sqs.create_queue(QueueName='test', Attributes={'DelaySeconds': '5'})
    return queue

def send_message(client):
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
        '16_05'
        )
    )
    print(response['MessageId'])



def receive_message(client):
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

def aws_file():
    with open('my-deployment-package.zip', 'rb') as file_data:
        bytes_content = file_data.read()
    return bytes_content


def create_lambda(client):
    response = client.create_function(
    Role='arn:aws:iam:: 123456789012:role/lambda-rol',
    FunctionName='function1',
    Runtime='python3.8',
    Handler='lambda_function.lambda_handler',
    Code=dict(ZipFile=aws_file()),
    Timeout=900,
    Environment={
        'Variables': {
            'Name': 'function1',
            'Environment': 'prod'
        }
    },
    )

    print(response)

def trigger_lambda(client):
    response = client.create_event_source_mapping(
        EventSourceArn=f'arn:aws:sqs:{AWS_REGION}:000000000000:test',
        FunctionName='function1',
        Enabled=True,
        BatchSize=1
)
    print(response)

def delete_lambda(client):
    response = client.delete_function(
    FunctionName='function1'
)



create_sqs()
create_lambda(create_client('lambda'))
trigger_lambda(create_client('lambda'))
send_message(create_client('sqs'))
receive_message(create_client('sqs'))

# delete_lambda(create_client('lambda'))

