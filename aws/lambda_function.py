import boto3
import pandas as pd
import io
from decimal import Decimal
import json


def lambda_handler(event, context):
    msg = event['Records'][0]['body']
    print(msg)

    dynamodb = boto3.resource('dynamodb', 
        region_name='us-east-1',
        aws_access_key_id='xyz',
        aws_secret_access_key='aaa',
        endpoint_url='http://localstack:4566')

    table = dynamodb.Table(msg)

    client = boto3.client('s3', 
        region_name="us-east-1",
        aws_access_key_id='xyz',
        aws_secret_access_key='aaa', 
        endpoint_url='http://localstack:4566')

    csv_obj = client.get_object(Bucket="test-bucket", Key=msg+'.csv')
    body = csv_obj['Body']
    df = pd.read_csv(io.BytesIO(body.read()))

    with table.batch_writer() as batch:
        for i, row in df.iterrows():
            data = row.to_dict()
            ddb_data = json.loads(json.dumps(data), parse_float=Decimal)
            for item_key in ddb_data: 
                ddb_data[item_key] = str(ddb_data[item_key])
            batch.put_item(Item=ddb_data)
    print("DynamoDB put_items completed")

    
    return "msg"