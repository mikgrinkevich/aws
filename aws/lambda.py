import requests

def lambda_handler(event, context):
    # message = event['Records'][0]['Sqs']['Message']
    # print("From SQS: " + message)
    response = requests.get("https://www.example.com/")
    print(response.text)
    print(event)
    return response.text