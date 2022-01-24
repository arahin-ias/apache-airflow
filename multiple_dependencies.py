import airflow

from airflow import DAG
from airflow.operators.dummy import DummyOperator

dag = DAG(
    dag_id="fan-out-dependency",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval='@daily',
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

fetch_sales = DummyOperator(
    task_id='fetch_sales',
    dag=dag,
)
clean_sales = DummyOperator(
    task_id='clean_sales',
    dag=dag,
)
