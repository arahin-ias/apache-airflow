import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator

BUCKET_NAME = 'mnist-bucket-optimus'


def create_bucket():
    hook = S3Hook(aws_conn_id='aws_credentials')
    hook.create_bucket(bucket_name=BUCKET_NAME)


dag = DAG(
    dag_id='aws_handwritten_digit_classifier',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(3),
)

create_s3_bucket = PythonOperator(
    task_id='create_s3_bucket_for_mnist_data',
    python_callable=create_bucket,
    dag=dag,
)

download_mnist_data = S3CopyObjectOperator(
    task_id='download_mnist_data',
    source_bucket_name="sagemaker-sample-data-eu-west-1",
    source_bucket_key="algorithms/kmeans/mnist/mnist.pkl.gz",
    dest_bucket_name=BUCKET_NAME,
    dest_bucket_key="mnist.pkl.gz",
    dag=dag,
)

create_s3_bucket >> download_mnist_data
