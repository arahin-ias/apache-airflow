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

SOURCE_DIRECTORY = '/home/rahin/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data/'
DESTINATION_DIRECTORY = '/home/rahin/output/'

dag = DAG(
    dag_id="flight_data_spark_job",
    start_date=dt.datetime(2022, 2, 1),
    schedule_interval=None,
)

build_jar = BashOperator(
    task_id='build_spark_jar',
    bash_command='mvn clean install -f ~/source-code/Intellij-Project/Spark-Flights-Data-Analysis/pom.xml',
    dag=dag,
)


def run_all_spark_job():
    for job_id in range(1, 7):
        submit_spark_job = BashOperator(
            task_id=f'submit_spark_job_{job_id}',
            bash_command=f'spark-submit --class org.flight.analysis.FlightDataProcessor '
                         '--master spark://ubuntu:7077 '
                         '--deploy-mode cluster '
                         '--executor-memory 16G '
                         '--total-executor-cores 12 '
                         '--driver-memory 16G '
                         '--driver-cores 12 '
                         '~/source-code/Intellij-Project/Spark-Flights-Data-Analysis/target/spark-flights'
                         '-data-analysis-1.0-SNAPSHOT.jar '
                         '~/source-code/Intellij-Project/Spark-Flights-Data-Analysis/2015_flights_data/ '
                         f'~/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data {job_id}',
            dag=dag,
        )
        submit_spark_job.execute(dict())


spark_job = PythonOperator(
    task_id='spark_job_runner',
    python_callable=run_all_spark_job,
    dag=dag
)


def make_tarfile(destination_dir, filename, source_dir):
    with tarfile.open(destination_dir + filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


def find_all_files(root_dir):
    file_list = glob.iglob(root_dir + '**/**', recursive=True)
    return file_list


def filter_all_success_directory(file_list):
    success_files_list = filter(
        lambda x: (
            x.endswith('_SUCCESS')
        )
        , file_list
    )
    return success_files_list


def find_success_files_parent(success_file_directory):
    success_files_parents = set(
        map(
            lambda file: Path(file).parent,
            success_file_directory
        )
    )
    return success_files_parents


def compress_output_file(source, destination):
    all_files = find_all_files(root_dir=source)

    all_success_file_dir = filter_all_success_directory(all_files)

    success_files_parents = find_success_files_parent(all_success_file_dir)

    for f in success_files_parents:
        make_tarfile(destination_dir=destination, filename=os.path.basename(f) + '.tar',
                     source_dir=source + '/' + str(os.path.basename(f)))


clean_output_directory = BashOperator(
    task_id='clean_output_directory',
    bash_command='rm -rf ~/S3UploadData',
    dag=dag,
)

create_directory_task = BashOperator(
    task_id='create_directory_task',
    bash_command='mkdir -p ~/S3UploadData',
    dag=dag,
)

compress_task = PythonOperator(
    task_id='compress_task',
    python_callable=compress_output_file,
    op_kwargs={"source": SOURCE_DIRECTORY, 'destination': DESTINATION_DIRECTORY},
    dag=dag,
)

build_jar >> spark_job >> clean_output_directory >> create_directory_task >> compress_task
