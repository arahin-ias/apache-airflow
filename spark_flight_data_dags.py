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

BUCKET_NAME_OPERATOR = 'spark-flight-data-bucket-operator'
BUCKET_NAME_S3HOOK = 'spark-flight-data-bucket-s3hook'
ROOT_DIRECTORY = '/home/rahin'
SOURCE_DIRECTORY = f'{ROOT_DIRECTORY}/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data/'
DESTINATION_DIRECTORY = f'{ROOT_DIRECTORY}/S3UploadData/'

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


def upload_files(source_dir, bucket):
    upload_files_list = list_of_upload_files(source_dir)
    for file in upload_files_list:
        upload_file(file, bucket)


def create_bucket(**context):
    hook = S3Hook(aws_conn_id='aws_default')
    hook.create_bucket(bucket_name=context['bucket_name'])


def load_data(**context):
    bucket_name = context['bucket_name']
    file_path = context['file_path']
    list_of_file_to_uploaded = list_of_upload_files(file_path)

    hook = S3Hook(aws_conn_id='aws_default')
    for file in list_of_file_to_uploaded:
        hook.load_file(
            filename=f'{file}',
            key=f'spark/{os.path.basename(file)}',
            bucket_name=bucket_name,
        )


with DAG(
        dag_id="flight_data_spark_job",
        start_date=dt.datetime(2022, 2, 1),
        schedule_interval=None,
) as dag:
    build_jar = BashOperator(
        task_id='build_spark_jar',
        bash_command=f'mvn clean install -f '
                     f'{ROOT_DIRECTORY}/source-code/Intellij-Project/Spark-Flights-Data-Analysis/pom.xml',
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
                         f'{ROOT_DIRECTORY}/source-code/Intellij-Project/Spark-Flights-Data-Analysis/target/'
                         f'spark-flights-data-analysis-1.0-SNAPSHOT.jar '
                         f'{ROOT_DIRECTORY}/source-code/Intellij-Project/Spark-Flights-Data-Analysis/2015_flights_data/ '
                         f'{ROOT_DIRECTORY}/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data '
                         f'{spark_job_id}',
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

    create_bucket_dummy_task = DummyOperator(
        task_id='file_upload_dummy_task',
    )

    spark_submit_dummy_task >> clean_output_directory >> create_directory_task >> \
    compress_task >> create_bucket_dummy_task

    create_bucket_S3_operator = S3CreateBucketOperator(
        task_id='create_bucket_S3_operator',
        bucket_name=BUCKET_NAME_OPERATOR,
        region_name='us-east-1',
    )

    create_bucket_s3_hook = PythonOperator(
        task_id='create_bucket_s3_hook',
        python_callable=create_bucket,
        op_kwargs={'bucket_name': BUCKET_NAME_S3HOOK}
    )

    create_bucket_dummy_task >> [create_bucket_S3_operator, create_bucket_s3_hook]

    upload_file_dummy_task = DummyOperator(
        task_id='upload_file_dummy_task',
    )

    create_bucket_S3_operator >> upload_file_dummy_task

    upload_files_s3_hook = PythonOperator(
        task_id='upload_files_s3_hook',
        python_callable=load_data,
        op_kwargs={
            'bucket_name': BUCKET_NAME_S3HOOK,
            'file_path': f'{ROOT_DIRECTORY}/S3UploadData/'
        }
    )

    create_bucket_s3_hook >> upload_files_s3_hook

    upload_files_boto_3_client = PythonOperator(
        task_id=f'upload_files_boto_3_client',
        python_callable=upload_files,
        op_kwargs={
            'source_dir': f'{ROOT_DIRECTORY}/S3UploadData/',
            'bucket': BUCKET_NAME_OPERATOR},
    )

    upload_file_dummy_task >> upload_files_boto_3_client
