import gzip
import io
import pickle

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from sagemaker.amazon.common import write_numpy_to_dense_tensor

BUCKET_NAME = 'mnist-bucket-optimus'

dag = DAG(
    dag_id='aws_handwritten_digit_classifier',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(3),
)


def create_bucket():
    hook = S3Hook(aws_conn_id='aws_credentials')
    hook.create_bucket(bucket_name=BUCKET_NAME)


def extract_mnist_data():
    s3hook = S3Hook(aws_conn_id='aws_credentials')

    mnist_buffer = io.BytesIO()
    mnist_obj = s3hook.get_key(bucket_name="your-bucket", key="mnist.pkl.gz")
    mnist_obj.download_fileobj(mnist_buffer)

    mnist_buffer.seek(0)
    with gzip.GzipFile(fileobj=mnist_buffer, mode="rb") as f:
        train_set, _, _ = pickle.loads(f.read(), encoding="latin1")
        output_buffer = io.BytesIO()
        write_numpy_to_dense_tensor(
            file=output_buffer, array=train_set[0], labels=train_set[1]
        )
        output_buffer.seek(0)
        s3hook.load_file_obj(
            output_buffer, key="mnist_data", bucket_name=BUCKET_NAME, replace=True
        )


extract_mnist_data = PythonOperator(
    task_id='extract_mnist_data',
    python_callable=extract_mnist_data,
    dag=dag,
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
