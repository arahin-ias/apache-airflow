import datetime as dt
from pathlib import Path

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

with DAG(
        dag_id="check_file_sensor",
        start_date=dt.datetime(2022, 2, 1),
        schedule_interval=None,
) as dag:
    check_file_task = FileSensor(
        task_id='file_sensor',
        filepath='/home/rahin/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data'
                 '/find_average_departure_delay/_SUCCESS'
    )
