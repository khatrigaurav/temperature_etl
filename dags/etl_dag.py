import pandas as pd
import requests

from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from download_data import get_temperature, put_to_s3


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    }

dag = DAG(
    'weather_dag',
    default_args=default_args,
    description='ETL DAG for weather data',
)

download_data = PythonOperator(task_id='download_data',
                                 python_callable=get_temperature,
                                 dag=dag)

put_data_to_s3 = PythonOperator(task_id='put_data_to_s3',
                                    python_callable=put_to_s3,
                                    dag=dag)

download_data >> put_data_to_s3
