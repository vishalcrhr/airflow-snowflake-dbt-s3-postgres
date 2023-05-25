from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator 
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
    'email': ['adsd@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

Bucket_name = Variable.get("BUCKET_NAME")

query_test = """select * from S3_TO_SNOWFLAKE.COMBINED_DATA;"""
query_create_format = """CREATE OR REPLACE FILE FORMAT mycsvformat
   TYPE = 'CSV'
   FIELD_DELIMITER = ','
   SKIP_HEADER = 1;"""

query_change_warehouse =f"""alter """

query_stage = f"""CREATE OR REPLACE STAGE my_csv_stage
  FILE_FORMAT = mycsvformat
  URL = 's3://{Bucket_name}' 
  credentials = (aws_key_id = 'hjhjh' aws_secret_key = 'kjkjkj')
  ; 
  """
query_list_stage = f""" list @my_csv_stage;"""

query_copy_data = """COPY INTO S3_TO_SNOWFLAKE.COMBINED_DATA
FROM (select $1,$2,$3,$4,TO_DATE($5,'YYYY-MM-DD'),$6 from @my_csv_stage/COMBINED_BOOKINGS.csv)
  ON_ERROR = 'skip_file'  FILE_FORMAT = (FORMAT_NAME = mycsvformat)"""

query_change_warehouse =f"""alter """

query = """select * from COMBINED_BOOKINGS limit 10000;"""

dag = DAG(
    'S3_TO_SNOWFLAKE',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False
)
 

def execute_snowflake(sql, snowflake_conn_id, with_cursor=False):
    """Execute snowflake query."""
    hook_connection = SnowflakeHook(
        snowflake_conn_id=snowflake_conn_id
    )

    with closing(hook_connection.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(sql)
            res = cur.fetchall()
            if with_cursor:
                return (res, cur)
            else:
                return res

# Connection Test
snowflake = SnowflakeOperator(
    task_id='test_snowflake_connection',
    sql=query_test,
    snowflake_conn_id='snow',
    dag=dag
)

create_file_format = SnowflakeOperator(
    task_id='create_file_format',
    sql=query_create_format,
    snowflake_conn_id='snow',
    dag=dag
)

create_stage = SnowflakeOperator(
    task_id='create_stage',
    sql=query_stage,
    snowflake_conn_id='snow',
    dag=dag
)

list_stage = SnowflakeOperator(
    task_id='list_stage',
    sql=query_list_stage,
    snowflake_conn_id='snow',
    dag=dag
)
copy_data = SnowflakeOperator(
    task_id='copy_data',
    sql=query_copy_data,
    snowflake_conn_id='snow',
    dag=dag
)

snowflake >> create_file_format >> create_stage >> list_stage>> copy_data
