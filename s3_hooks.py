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
    dag_id='aws_s3_hooks',
    start_date=dt.datetime(2022, 2, 1),
    schedule_interval=None,
)
