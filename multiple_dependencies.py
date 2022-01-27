import airflow

from airflow import DAG
from airflow.operators.dummy import DummyOperator

dag = DAG(
    dag_id="fan-out-dependency",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval='@daily',
)

start = DummyOperator(task_id="start", dag=dag)

fetch_sales = DummyOperator(task_id="fetch_sales", dag=dag)
clean_sales = DummyOperator(task_id="clean_sales", dag=dag)

fetch_weather = DummyOperator(task_id="fetch_weather", dag=dag)
clean_weather = DummyOperator(task_id="clean_weather", dag=dag)

join_datasets = DummyOperator(task_id="join_datasets", dag=dag)
train_model = DummyOperator(task_id="train_model", dag=dag)
deploy_model = DummyOperator(task_id="deploy_model", dag=dag)

start >> [fetch_sales, fetch_weather]
fetch_sales >> clean_sales
fetch_weather >> clean_weather
[clean_sales, clean_weather] >> join_datasets
join_datasets >> train_model >> deploy_model
