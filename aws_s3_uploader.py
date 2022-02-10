import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='aws_s3_bucket_create',
    start_date=dt.datetime(2022, 2, 1),
    schedule_interval=None,
)

create_aws_bucket = S3CreateBucketOperator(
    task_id='create_s3_buckets',
    bucket_name='spark-data-ware-house',
    region_name='us-east-1',

)
