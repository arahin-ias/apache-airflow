import datetime as dt
from pathlib import Path
import logging
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
import glob
import os
import tarfile
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from airflow.sensors.python import PythonSensor

dag = DAG(
    dag_id='aws_s3_bucket_create',
    start_date=dt.datetime(2022, 2, 1),
    schedule_interval=None,
)

create_bucket = S3CreateBucketOperator(
    task_id='s3_bucket_dag_create',
    bucket_name='adnan-test-bucket-1',
    region_name='us-east-1',
    dag=dag,
)


def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


upload_files = PythonOperator(
    task_id='upload_files',
    python_callable=upload_file,
    op_kwargs={'file_name': '/home/rahin/S3UploadData/find_average_departure_delay.tar',
               'bucket': 'adnan-test-bucket-1'},
    dag=dag,
)

create_bucket >> upload_files
