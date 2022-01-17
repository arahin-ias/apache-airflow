import airflow
from airflow.operators.python import PythonOperator
from airflow import DAG

dag = DAG(
    dag_id="print_test",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
)


def _print_context(**context):
    start = context["execution_date"]
    end = context["next_execution_date"]
    print(f"Start: {start}, end: {end}")


print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=dag
)
