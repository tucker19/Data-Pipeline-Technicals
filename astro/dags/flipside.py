from flipside_utils.operators.flipsideoperator import FlipsideOperator
from default_utils import default_args
from airflow.operators.empty import EmptyOperator

from airflow import DAG
from datetime import datetime, timedelta

default_args = default_args

with DAG(dag_id='flipside',
         catchup=False,
         default_args=default_args,
         schedule_interval=None) as dag:
    begin = EmptyOperator(task_id="begin")

    flipside_ingest = FlipsideOperator(
        task_id="flipside-ingest")

    end = EmptyOperator(task_id="end")

    begin >> flipside_ingest >> end