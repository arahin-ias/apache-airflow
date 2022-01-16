import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="01_unscheduled",
    start_date=dt.datetime(2019, 1, 1),
    schedule_interval=None,
)

fetch_events = BashOperator(
    task_id="fetch_event",
    bash_command=(
        "mkdir -p /home/airflow/airflow_output/data && curl -o /home/airflow/airflow_output/data/events.json "
        "https://localhost:5000/events"
    ),
    dag=dag,
)
