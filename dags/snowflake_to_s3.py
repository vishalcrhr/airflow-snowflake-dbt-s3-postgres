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

query_test = """select current_date() as date;"""
query = """select * from COMBINED_BOOKINGS limit 10000;"""

dag = DAG(
    'SNOWFLAKE_TO_S3',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False
)
 
# Connection Test
snowflake = SnowflakeOperator(
    task_id='test_snowflake_connection',
    sql=query_test,
    snowflake_conn_id='snow',
    dag=dag
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

def snowflake_to_pandas(query, snowflake_conn_id,**kwargs): 
    """Convert snowflake list to df."""
    result, cursor = execute_snowflake(query, snowflake_conn_id, True)
    headers = list(map(lambda t: t[0], cursor.description))
    df = pd.DataFrame(result)
    df.columns = headers

    # save file before to send 
    
    # NOTE : This is not recommended in case of multi-worker deployments
    df.to_csv('data.csv',header=True,mode='w',sep=',', index=False)

        # Send to S3
    
    hook = S3Hook(aws_conn_id="s3con")
    hook.load_file(
        filename='data.csv',
        key='COMBINED_BOOKINGS.csv',
        bucket_name=Variable.get("BUCKET_NAME"),
        replace=True,
    )

    return 'This File Sent Successfully'


# Data Upload  Task 
upload_stage = PythonOperator(task_id='UPLOAD_FILE_INTO_S3_BUCKET',     
                             python_callable=snowflake_to_pandas,
                             op_kwargs={"query":query,"snowflake_conn_id":'snow'},     
                             dag=dag )


snowflake >> upload_stage 
