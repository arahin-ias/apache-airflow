import airflow
from airflow.utils import timezone, dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

ERP_CHANGE_DATE = airflow.utils.dates.days_ago(1)


def _fetch_sales(**context):
    if context["execution_date"] < ERP_CHANGE_DATE:
        _fetch_sales_old(**context)
    else:
        _fetch_sales_new(**context)


def _fetch_sales_old(**context):
    print("Fetching sales data (OLD)...")


def _fetch_sales_new(**context):
    print("Fetching sales data (NEW)...")


def _clean_sales(**context):
    if context["execution_date"] < airflow.utils.dates.days_ago(1):
        _clean_sales_old(**context)
    else:
        _clean_sales_new(**context)


def _clean_sales_old(**context):
    print("Preprocessing sales data (OLD)...")


def _clean_sales_new(**context):
    print("Preprocessing sales data (NEW)...")
