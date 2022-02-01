import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="flight_data_spark_job",
    start_date=dt.datetime(2019, 1, 1),
    schedule_interval="@hourly",
)

build_jar = BashOperator(
    task_id='build_spark_jar',
    bash_command='mvn clean install -f ~/source-code/Intellij-Project/Spark-Flights-Data-Analysis/pom.xml',
    dag=dag,
)

