from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from func import upload_to_s3, processed_by_pyspark_csv_saver


with DAG('push_to_s3', description='cloud dag',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 8, 15), catchup=False) as dag:

    upload_raw_files_to_csv = PythonOperator(
            task_id='upload_raw_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'bucket_name': 'test-bucket',
                'subname': 'data/'
            }
        )

    process_raw_data_with_pyspark = PythonOperator(
            task_id='process_pyspark',
            python_callable=processed_by_pyspark_csv_saver,
            op_kwargs={
                'bucket_name': 'test-bucket'
            }
        )

    upload_pyspark_processed_to_s3 = PythonOperator(
            task_id='upload_pyspark_processed_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'bucket_name': 'test-bucket',
                'subname': 'data/processed_'
            }
        )

upload_raw_files_to_csv >> process_raw_data_with_pyspark >> upload_pyspark_processed_to_s3

# s3 = S3Hook(aws_conn_id='MyS3Conn')
# s3.load_string('123', key='xyz',bucket_name='test-bucket', replace=True)   


#  {aws_access_key_id:"xyz", "aws_secret_access_key": "aaa", "aws_region": "eu-west-2", "host": "http://localhost:4566"}
# http://localhost:4566
#  {aws_access_key_id:"xyz", "aws_secret_access_key": "aaa"}
