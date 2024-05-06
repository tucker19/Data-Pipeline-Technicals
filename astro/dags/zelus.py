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
        continue_run=False,
        local_save=False,
        limit_ingestion=False,
        dbname='technicals',
        user='admin',
        password='admin',
        host='192.168.86.36',
        port=1234)

    end = EmptyOperator(task_id="end")

    begin >> zelus_ingest >> end