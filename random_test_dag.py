import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="test_random",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)


def matrix(**contest):
    print('matrix')


def print_loop(**context):
    print('print_loop')


create_metrics = PythonOperator(task_id="create_metrics", python_callable=matrix, dag=dag)

for supermarket_id in [1, 2, 3, 4]:
    process = PythonOperator(
        task_id=f"process_supermarket_{supermarket_id}",
        python_callable=print_loop,
        dag=dag,
    )
    create_metrics >> process
