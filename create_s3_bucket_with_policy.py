import datetime as dt
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.operators.python import PythonOperator
import glob
import os
import tarfile
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from airflow.sensors.python import PythonSensor

def create_bucket(**context):
    hook = S3Hook(aws_conn_id='aws_default')
    hook.create_bucket(bucket_name=context['bucket_name'])

dag = DAG(
    dag_id='aws_s3_bucket_create',
    start_date=dt.datetime(2022, 2, 1),
    schedule_interval=None,
)

hook = S3Hook(aws_conn_id='aws_default')
lr = hook.list_keys(bucket_name='spark-flight-data-bucket')


