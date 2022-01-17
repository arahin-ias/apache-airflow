from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag = DAG(dag_id='bash_dag', schedule_interval=None, start_date=datetime(2020, 1, 1), catchup=False)
# Task 1
dummy_task = DummyOperator(task_id='dummy_task', dag=dag)
# Task 2
bash_task = BashOperator(task_id='bash_task', bash_command="echo 'command executed from BashOperator'", dag=dag)

test_task = BashOperator(task_id='test_bash_command', bash_command='touch megatron.txt')

new_task = BashOperator(task_id='testing_dag_create_new_dir', bash_command='mkdir /home/airflow/test')

dummy_task >> bash_task >> test_task >> new_task
