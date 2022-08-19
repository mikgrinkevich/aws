#!/usr/bin/env bash
printf "Configuring localstack components..."

readonly LOCALSTACK_S3_URL=http://localstack:4566
sleep 5;

aws configure set aws_access_key_id xyz
aws configure set aws_secret_access_key aaa
echo "[default]" > ~/.aws/config
echo "region = eu-west-2" >> ~/.aws/config

printf "Creating a bucket test-bucket"
aws --endpoint-url=$LOCALSTACK_S3_URL s3api create-bucket --bucket test-bucket

echo "Create SQS queue testQueue"
aws \
  sqs create-queue \
  --queue-name testQueue \
  --endpoint-url http://localhost:4566 

