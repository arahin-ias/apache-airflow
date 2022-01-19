import airflow
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG

dag = DAG(
    dag_id="page_rank_counter",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@hourly",
)

extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip --force /home/airflow/airflow_output/wikipedia/stocksense/wikipageviews.gz"
)


def _fetch_pageviews(pagenames):
    result = dict.fromkeys(pagenames, 0)
    with open(f"/home/airflow/airflow_output/wikipedia/stocksense/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts
    print(result)


fetch_pageview = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "pagenames": {
            "Google",
            "Amazon",
            "Apple",
            "Microsoft",
            "Facebook"
        }
    },
    dag=dag,
)

extract_gz >> fetch_pageview
