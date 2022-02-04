import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="flight_data_spark_job",
    start_date=dt.datetime(2022, 2, 1),
    schedule_interval="@hourly",
)

build_jar = BashOperator(
    task_id='build_spark_jar',
    bash_command='mvn clean install -f ~/source-code/Intellij-Project/Spark-Flights-Data-Analysis/pom.xml',
    dag=dag,
)


def run_all_spark_job():
    for job_id in range(1, 7):
        submit_spark_job = BashOperator(
            task_id=f'submit_spark_job_{job_id}',
            bash_command=f'spark-submit --class org.flight.analysis.FlightDataProcessor '
                         '--master spark://ubuntu:7077 '
                         '--deploy-mode cluster '
                         '--executor-memory 16G '
                         '--total-executor-cores 12 '
                         '--driver-memory 16G '
                         '--driver-cores 12 '
                         '~/source-code/Intellij-Project/Spark-Flights-Data-Analysis/target/spark-flights'
                         '-data-analysis-1.0-SNAPSHOT.jar '
                         '~/source-code/Intellij-Project/Spark-Flights-Data-Analysis/2015_flights_data/ '
                         f'~/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data {job_id}',
            dag=dag,
        )
        submit_spark_job.execute(dict())


spark_job = PythonOperator(
    task_id='spark_job_runner',
    python_callable=run_all_spark_job,
    dag=dag
)

build_jar >> spark_job
