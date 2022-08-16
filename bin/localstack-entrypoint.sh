#!/usr/bin/env bash
printf "Configuring localstack components..."

readonly LOCALSTACK_S3_URL=http://localstack:4566
sleep 5;

aws configure set aws_access_key_id xyz
aws configure set aws_secret_access_key aaa

aws --endpoint-url=$LOCALSTACK_S3_URL s3api create-bucket --bucket test-bucket
