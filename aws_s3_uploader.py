import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
import os
import boto3

dag = DAG(
    dag_id='aws_s3_bucket_create',
    start_date=dt.datetime(2022, 2, 1),
    schedule_interval=None,
)

create_bucket = S3CreateBucketOperator(task_id='s3_bucket_dag_create', region_name='us-east-1')

# s3 = boto3.resource('s3')
#
# for bucket in s3.buckets.all():
#     print(bucket.name)

#
# s3_resource = boto3.resource("s3", region_name="us-east-1")
#
#
# def upload_objects():
#     try:
#         bucket_name = "test-bucket-spark"
#         root_path = '/home/rahin/S3UploadData'
#         my_bucket = s3_resource.Bucket(bucket_name)
#
#         for path, subdirs, files in os.walk(root_path):
#             path = path.replace("\\", "/")
#             directory_name = path.replace(root_path, "")
#             for file in files:
#                 my_bucket.upload_file(os.path.join(path, file), directory_name + '/' + file)
#
#     except Exception as err:
#         print(err)
#
#
# test = PythonOperator(
#     task_id='upload_to_s3',
#     python_callable=upload_objects,
#     dag=dag,
# )
#
# create_aws_bucket >> test
