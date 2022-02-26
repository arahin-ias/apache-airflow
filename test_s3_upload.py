import gzip
import io
import pickle
import datetime as dt
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from sagemaker.amazon.common import write_numpy_to_dense_tensor

BUCKET_NAME = 'mnist-bucket-optimus'


def extract_mnist_data():
    s3hook = S3Hook(aws_conn_id='aws_credentials')

    mnist_buffer = io.BytesIO()
    mnist_obj = s3hook.get_key(bucket_name=BUCKET_NAME, key="mnist.pkl.gz")
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


dag = DAG(
    dag_id='bucket_create_optimus',
    schedule_interval=None,
    start_date=dt.datetime(2022, 2, 1),
)
