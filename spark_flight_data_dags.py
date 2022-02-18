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

SOURCE_DIRECTORY = '/home/rahin/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data/'
DESTINATION_DIRECTORY = '/home/rahin/S3UploadData/'

transformation_data_list = ['number_of_cancelled_flights',
                            'find_total_number_departure_flight',
                            'find_most_cancelled_airline',
                            'find_average_departure_delay',
                            'find_total_distance_flown',
                            'find_origin_and_dest_by_max_distance']


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


def wait_for_data_to_generate(data_path):
    flight_data = Path(data_path)
    data_files = flight_data.glob("*.parquet")
    success_file = flight_data / "_SUCCESS"
    logging.info(f"{data_path}/_SUCCESS Files Found") if success_file.exists() else logging.info(
        f"{data_path}/_SUCCESS Files Not Found")

    return data_files and success_file.exists()


def list_of_upload_files(source):
    upload_files_list = find_all_files(source)

    tar_file = filter(
        lambda x: (
            x.endswith('tar')
        )
        , upload_files_list
    )

    return tar_file


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


with DAG(
        dag_id="flight_data_spark_job",
        start_date=dt.datetime(2022, 2, 1),
        schedule_interval=None,
) as dag:
    build_jar = BashOperator(
        task_id='build_spark_jar',
        bash_command='mvn clean install -f ~/source-code/Intellij-Project/Spark-Flights-Data-Analysis/pom.xml',
    )

    build_jar_dummy_task = DummyOperator(
        task_id='build_jar_dummy_task'
    )

    spark_submit_dummy_task = DummyOperator(
        task_id='spark_submit_dummy_task',
    )

    build_jar >> build_jar_dummy_task

    for spark_job_id in range(1, 7):
        submit_spark_job = BashOperator(
            task_id=f'submit_spark_job_{spark_job_id}',
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
                         f'~/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data {spark_job_id}',
        )

        check_file = PythonSensor(
            task_id=f'file_sensor_{spark_job_id}',
            python_callable=wait_for_data_to_generate,
            op_kwargs={'data_path': SOURCE_DIRECTORY + transformation_data_list[spark_job_id - 1]},
            mode="reschedule",
            dag=dag,
        )

        build_jar_dummy_task >> submit_spark_job >> check_file >> spark_submit_dummy_task

    clean_output_directory = BashOperator(
        task_id='clean_output_directory',
        bash_command='rm -rf ~/S3UploadData',
    )

    create_directory_task = BashOperator(
        task_id='create_directory_task',
        bash_command='mkdir -p ~/S3UploadData',
    )

    compress_task = PythonOperator(
        task_id='compress_task',
        python_callable=compress_output_file,
        op_kwargs={"source": SOURCE_DIRECTORY, 'destination': DESTINATION_DIRECTORY},
    )

    all_upload_files_list = list_of_upload_files('/home/rahin/S3UploadData/')

    create_bucket_dummy_task = DummyOperator(
        task_id='file_upload_dummy_task',
    )

    spark_submit_dummy_task >> clean_output_directory >> create_directory_task >> \
    compress_task >> create_bucket_dummy_task

    create_bucket = S3CreateBucketOperator(
        task_id='s3_bucket_dag_create',
        bucket_name='spark-flight-data-bucket',
        region_name='us-east-1',
    )

    create_bucket_dummy_task >> create_bucket

    for file in all_upload_files_list:
        file_name = os.path.basename(file)
        upload_files = PythonOperator(
            task_id=f'upload_files_{file_name}',
            python_callable=upload_file,
            op_kwargs={'file_name': file,
                       'bucket': 'spark-flight-data-bucket'},
        )
        create_bucket >> upload_files
