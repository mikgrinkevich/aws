from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from func import upload_raw_to_s3, upload_pyspark_to_s3, processed_by_pyspark_csv_saver


with DAG('push_to_s3', description='cloud dag',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 9, 5), catchup=False) as dag:

                    
    upload_raw_files_to_csv = PythonOperator(
            task_id='upload_raw_to_s3',
            python_callable=upload_raw_to_s3,
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
            python_callable=upload_pyspark_to_s3,
            op_kwargs={
                'bucket_name': 'test-bucket',
                'subname': 'data/processed_'
            }
        )

upload_raw_files_to_csv >> process_raw_data_with_pyspark >> upload_pyspark_processed_to_s3
