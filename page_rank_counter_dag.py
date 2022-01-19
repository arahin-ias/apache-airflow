import airflow
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG

dag = DAG(
    dag_id="user_define_callable",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@hourly",
)
