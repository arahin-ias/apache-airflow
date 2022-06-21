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
from botocore.exceptions import ClientError
from airflow.sensors.python import PythonSensor
import logging

ARTIFACT_LOCATION = "/Users/arahin/sourcecode/sample-code/Spark-Flights-Data-Analysis/data-extract-processor/target/data-extract-processor-1.0-SNAPSHOT.jar"
SOURCE_DATA_LOCATION = "s3://arahin-spark-test-bucket/data/"

JOB_FLOW_OVERRIDES = {
    "Name": "adnan_airflow_cluster",
    "ReleaseLabel": "emr-5.30.0",
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

with DAG(
        dag_id="create_emr_cluster",
        start_date=dt.datetime(2022, 2, 1),
        schedule_interval=None,
) as dag:
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
        dag=dag,
    )
