from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import sqlalchemy


default_args = {
    'owner' : 'susi',
    'depend_on_past' : False,
    'start_date' : datetime(2023, 1, 20),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

with DAG('dags', default_args=default_args, schedule_interval=None, catchup=False) as dag:

engine = create_engine('postgresql://postgres:Juni031997@localhost:5432/postgres')
df.to_sql(file_name,engine)

