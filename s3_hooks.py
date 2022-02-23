from airflow.hooks.S3_hook import S3Hook


def create_bucket():
    hook = S3Hook(aws_conn_id='aws_credentials')
    hook.create_bucket(bucket_name='spark-flight-test-data-bucket')


def list_bucket():
    hook = S3Hook(aws_conn_id='aws_credentials')
    lr = hook.list_keys(bucket_name='spark-flight-data-bucket')
    print(lr)


def load_data():
    hook = S3Hook(aws_conn_id='aws_credentials')
    hook.load_file(
        filename='/home/rahin/S3UploadData/find_average_departure_delay.tar',
        key='spark/find_average_departure_delay.tar',
        bucket_name='spark-flight-test-data-bucket',
    )


create_bucket()
load_data()
list_bucket()
