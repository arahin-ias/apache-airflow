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

create_aws_bucket = S3CreateBucketOperator(
    task_id='create_s3_buckets',
    bucket_name='spark-data-ware-house',
    region_name='us-east-1',
    dag=dag,
)

s3_resource = boto3.resource("s3", region_name="us-east-1")


def upload_objects():
    try:
        bucket_name = "spark-data-ware-house"
        root_path = '/home/rahin/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data'
        my_bucket = s3_resource.Bucket(bucket_name)

        for path, subdirs, files in os.walk(root_path):
            path = path.replace("\\", "/")
            directory_name = path.replace(root_path, "")
            for file in files:
                my_bucket.upload_file(os.path.join(path, file), directory_name + '/' + file)

    except Exception as err:
        print(err)
