from fleetio_utils.operators.fleetio_operator  import FleetioOperator
from default_utils import default_args
from airflow.operators.empty import EmptyOperator

from airflow import DAG
from datetime import datetime, timedelta

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "1hxPf4fMbJevU47PfSX8O6_NVy081v8ninrH7K4ub6IA"
RANGE_NAME = "issues"
PUBLISH_LINK = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSK3cw1dAHgcrNna663hKuB_6_-u4PU4sPIA4vgokSarRmCRC9d1TvYQBA9mngYRycN_cAIfgpQUPa8/pub?output=csv"

default_args = default_args

with DAG(dag_id='fleetio_ingest', catchup = False, default_args=default_args, schedule_interval = None) as dag:
    begin = EmptyOperator(task_id="begin")

    fleetio_ingest = FleetioOperator(
        task_id="fleetio-ingest",
        scopes=SCOPES,
        spreadsheet_id=SPREADSHEET_ID,
        range_name=RANGE_NAME,
        publish_link=PUBLISH_LINK)

    end = EmptyOperator(task_id="end")

    begin >> fleetio_ingest >> end