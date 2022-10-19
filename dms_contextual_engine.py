from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
import glob
import os
import tarfile
from pathlib import Path
import boto3
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from botocore.exceptions import ClientError
from airflow.sensors.python import PythonSensor
import logging

LOCAL_ARTIFACT_LOCATION = '/Users/arahin/sourcecode/poc/spark-dms-contextual-pipeline-poc/target/spark-dms-contextual-pipeline-poc-1.0-SNAPSHOT.jar'

SPARK_STEPS = [
    {
        'Name': 'DmsContextualSparkJob',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--class', 'org.ias.spark.dailymarting.DmsContextualDriverEngine',
                's3://arahin-spark-test-bucket/artifact/spark-dms-contextual-pipeline-poc-1.0-SNAPSHOT.jar',
                's3://arahin-spark-test-bucket/agg_small_test_data/*.gz',
                's3://arahin-spark-test-bucket/dms_aggregated_data'
            ]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    "Name": "adnan_airflow_cluster",
    "ReleaseLabel": "emr-6.8.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "Ec2KeyName": "adnan-emr-kp",
        "Ec2SubnetIds": ["subnet-0c58d28a7e0f05160"],
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "ON_DEMAND",  # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,  # this lets us programmatically terminate the cluster
    },
}


def load_data(**context):
    bucket_name = context['bucket_name']
    filename = context['filename']
    key = context['key']
    s3hook = S3Hook(aws_conn_id='aws_default')
    s3hook.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)


with DAG(
        dag_id="spark_dms_contextual_engine_job",
        start_date=dt.datetime(2022, 2, 1),
        schedule_interval=None,
) as dag:
    build_jar = BashOperator(
        task_id='build_spark_jar',
        bash_command=f'mvn clean install -f '
                     '/Users/arahin/sourcecode/poc/spark-dms-contextual-pipeline-poc/pom.xml',
    )

    upload_artifact_to_s3 = PythonOperator(
        task_id='upload_files_s3_hook',
        python_callable=load_data,
        op_kwargs={
            'bucket_name': 'arahin-spark-test-bucket',
            'filename': LOCAL_ARTIFACT_LOCATION,
            'key': f'artifact/{os.path.basename(LOCAL_ARTIFACT_LOCATION)}',
        }
    )

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
        dag=dag,
    )

    step_adder = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_STEPS,
        dag=dag,
    )

    last_step = len(SPARK_STEPS) - 1

    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[" + str(last_step) + "] }}",
        aws_conn_id="aws_default",
        dag=dag,
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        dag=dag,
    )

    build_jar >> upload_artifact_to_s3 >> create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
