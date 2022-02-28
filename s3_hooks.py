import os.path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import glob

ROOT_DIRECTORY = '/home/rahin'
SOURCE_DIRECTORY = f'{ROOT_DIRECTORY}/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data/'
DESTINATION_DIRECTORY = f'{ROOT_DIRECTORY}/S3UploadData/'


def create_bucket(**context):
    hook = S3Hook(aws_conn_id='aws_default')
    hook.create_bucket(bucket_name=context['bucket_name'])


def list_bucket():
    hook = S3Hook(aws_conn_id='aws_default')
    lr = hook.list_keys(bucket_name='spark-flight-data-bucket')
    print(lr)


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


def find_all_files(root_dir):
    file_list = glob.iglob(root_dir + '**/**', recursive=True)
    return file_list


def list_of_upload_files(source):
    upload_files_list = find_all_files(source)

    tar_file = filter(
        lambda x: (
            x.endswith('tar')
        )
        , upload_files_list
    )

    return tar_file


# file_list = list_of_upload_files(f'{ROOT_DIRECTORY}/S3UploadData/')

load_data(bucket_name='my-context-test-bucket', file_path=f'{ROOT_DIRECTORY}/S3UploadData/')
# list_bucket()
