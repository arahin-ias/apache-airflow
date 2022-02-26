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
import logging

from airflow.sensors.python import PythonSensor


def wait_for_data_to_generate(data_path):
    flight_data = Path(data_path)
    data_files = flight_data.glob("*.parquet")
    success_file = flight_data / "_SUCCESS"
    logging.info(f"{data_path}/_SUCCESS Files Found") if success_file.exists() else logging.info(
        f"{data_path}/_SUCCESS Files Not Found")

    return data_files and success_file.exists()


with DAG(
        dag_id="check_file_sensor",
        start_date=dt.datetime(2022, 2, 1),
        schedule_interval=None,
) as dag:
    check_file_task = PythonSensor(
        task_id='file_sensor',
        python_callable=wait_for_data_to_generate,
        op_kwargs={
            "data_path": "/home/rahin/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data"
                         "/find_most_cancelled_airline"},
        dag=dag,
    )
