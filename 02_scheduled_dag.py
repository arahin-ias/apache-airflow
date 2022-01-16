import airflow
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG

dag = DAG(
    dag_id="02_scheduled_dag",
    schedule_interval="@daily",
    start_date=dt.datetime(2019, 1, 1),
)

fetch_events = BashOperator(
    task_id="fetch_event",
    bash_command=(
        "mkdir -p /home/airflow/airflow_output/data_02 && curl -o /home/airflow/airflow_output/data_02/events.json "
        "http://localhost:5000/events "
    ),
    dag=dag,
)

def _calculate_stats(input_path, output_path):
    events = pd.read_json(input_path)
    stats = events.groupby(['date', 'user']).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": "/home/airflow/airflow_output/data_02/events.json",
        "output_path": "/home/airflow/airflow_output/data_02/stats.csv",
    },
    dag=dag,
)

fetch_events >> calculate_stats
