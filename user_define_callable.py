import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from urllib import request

dag = DAG(
    dag_id="user_define_callable",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@hourly",
)


def _get_data(output_path, **context):
    year, month, day, hour, *_ = context['execution_date'].timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_args=["/home/airflow/airflow_output/wikipedia/callable/wikipageviews.gz"],
    dag=dag,
)
