from zelus_utils.operators.zelus_operator  import ZelusOperator
from default_utils import default_args
from airflow.operators.empty import EmptyOperator

from airflow import DAG
from datetime import datetime, timedelta

default_args = default_args

with DAG(dag_id='zelus_ingest',
         catchup = False,
         default_args=default_args,
         schedule_interval = None) as dag:
    begin = EmptyOperator(task_id="begin")

    zelus_ingest = ZelusOperator(
        task_id="zelus-ingest",
        dbname='technicals')

    end = EmptyOperator(task_id="end")

    begin >> zelus_ingest >> end