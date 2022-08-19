import boto3
import pandas as pd
import io


def lambda_handler(event, context):
    msg = event['Records'][0]['body']
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table("msg")

    # get dataframe
    aws_session = boto3.Session()
    client = aws_session.client('s3', region_name="us-east-1")

    # get object
    csv_obj = client.get_object(Bucket="test", Key='data/'+msg+'.csv')
    body = csv_obj['Body']
    df = pd.read_csv(io.BytesIO(body.read()))

    # DynamoDB put_items called
    with table.batch_writer() as batch:
        for i, row in df.iterrows():
            batch.put_item(Item=row.to_dict())
    print("DynamoDB put_items completed")

    
    return "msg"



# message = event['Records'][0]['Sqs']['Message']
