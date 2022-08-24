import logging
from datetime import date, datetime
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# AWS_PROFILE = 'localstack'
# ENDPOINT_URL = os.environ.get('LOCALSTACK_S3_URL')

load_dotenv('/home/nikol/internship/task8/task8/.env')

AWS_REGION = os.environ.get('AWS_REGION')
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
LOCALSTACK_URL = os.environ.get('LOCALSTACK_S3_URL')

print(AWS_ACCESS_KEY,AWS_REGION, AWS_SECRET_KEY,LOCALSTACK_URL)

# logger config
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')

# boto3.setup_default_session(profile_name=AWS_PROFILE)

dynamodb_client = boto3.resource(
    'dynamodb', 
    endpoint_url=LOCALSTACK_URL,
    region_name=AWS_REGION, 
    aws_access_key_id=AWS_ACCESS_KEY, 
    aws_secret_access_key=AWS_SECRET_KEY,
    verify=False)

dates = ['20_09', '18_10', '16_07', '18_09', '18_11', '20_03', '16_05', 
    '18_08', '19_04', '17_06', '17_05', '18_06', '19_10', '20_06', 
    '16_11', '19_06', '20_10', '17_08', '16_06', '16_08', '19_08', 
    '20_07', '17_07', '18_05', '17_09', '18_04', '19_07', '19_11', 
    '18_07', '16_09', '20_05', '16_10', '17_10', '20_08', '20_11', 
    '20_04', '19_09', '19_05']


def create_dynamodb_table(table_name):
    """
    Creates a DynamoDB table.
    """
    try:
        response = dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'index',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'index',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            },
            Tags=[
                {
                    'Key': 'Helsinki data',
                    'Value': 'cloud-dynamodb-table'
                }
            ])

    except ClientError:
        logger.exception('Could not create the table.')
        raise
    else:
        return response

def main():
    """
    Main invocation function.
    """
    for val in dates:
        table_name = val
        logger.info('Creating a DynamoDB table...')
        dynamodb = create_dynamodb_table(table_name)
        logger.info(
            f'DynamoDB table created: {dynamodb}')

main()