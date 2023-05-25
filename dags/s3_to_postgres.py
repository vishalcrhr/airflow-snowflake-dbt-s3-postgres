from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from PostgresToS3Operator import PostgresToS3Operator

from airflow.models import Variable
from airflow import models
from contextlib import closing
import pandas as pd
import time
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator, S3DeleteBucketOperator

default_args = {
    'owner': 'Datexland',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 14),
    'email': ['xyz@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

query_test = """select current_date() as date;"""
query = """select * from COMBINED_BOOKINGS limit 10000;"""

dag = DAG(
    'S3_TO_POSTGRES',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False
)

test_postgres_con = PostgresToS3Operator(
  task_id='copy_postgres_to_s3',
  tablename='user',
  s3_bucket=f"""{Variable.get("bucket")}""",
  aws_con_id='s3con',
  postgres_conn_id='postgre_con',
  pre_sql='select * from log;',
  dag=dag
)
 
