import airflow
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator

dag = DAG(
    dag_id='aws_handwritten_digit_classifier',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(3),
)

download_mnist_data = S3CopyObjectOperator(
    task_id='download_mnist_data',
    source_bucket_name="sagemaker-sample-data-eu-west-1",
    source_bucket_key="algorithms/kmeans/mnist/mnist.pkl.gz",
    dest_bucket_name="your-bucket",
    dest_bucket_key="mnist.pkl.gz",
    dag=dag,
)
