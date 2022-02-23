import airflow
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator

dag = DAG(
    dag_id='aws_handwritten_digit_classifier',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(3),
)
