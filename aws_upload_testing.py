import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime as dt

from airflow.hooks.S3_hook import S3Hook

dag = DAG(
    dag_id='aws_s3_upload',
    start_date=dt.datetime(2022, 2, 1),
    schedule_interval=None
)


def create_bucket(bucket_name):
    s3_hookt = S3Hook(aws_conn_id='aws_default')
    s3_hookt.create_bucket(bucket_name=bucket_name)


def load(bucket_name):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_file(filename='/Users/arahin/test/text.txt', key='optimus/test.txt',
                      bucket_name=bucket_name)


BUCKET_NAME = 'arahin-test-dag-bucket'

create_bucket = PythonOperator(
    task_id='create_s3_bucket',
    python_callable=create_bucket,
    op_kwargs={'bucket_name': BUCKET_NAME},
    dag=dag
)

load_data = PythonOperator(
    task_id='load_data_to_bucket',
    python_callable=load,
    op_kwargs={'bucket_name': BUCKET_NAME},
    dag=dag,
)

create_bucket >> load_data
