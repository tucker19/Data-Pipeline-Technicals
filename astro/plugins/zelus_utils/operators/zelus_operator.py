from zelus import ZelusCricket
import glob
from airflow.models import BaseOperator
import logging
import psycopg2

logger = logging.getLogger()

class ZelusOperator(BaseOperator):
    def __init__(self):
        pass

    def __init__(self, *args, **kwargs):
        super(ZelusOperator, self).__init__(*args, **kwargs)

    def execute(self, context=None):
        zelus_testing = ZelusCricket()
        todays_date_str = zelus_testing.todays_date_dir()
        zelus_testing.get_data_from_site(todays_date_str)
        # zelus_testing.get_data_from_local(todays_date_str)

        jsonFiles = glob.glob(f"data/{todays_date_str}/*.json")
        cricketStats = []
        input_count = 0
        for jsonFilePath in jsonFiles:
            input_count += 1
            cricketStats.append(zelus_testing.get_data_from_json_file(todays_date_str, jsonFilePath))
            if input_count > 1:
                break

        local_results_csv = {}
        for key in ['info', 'match_registry', 'innings']:
            local_results_csv[key] = zelus_testing.get_result_destination(todays_date_str, key)

        conn = psycopg2.connect(
            dbname='zelus',
            user='admin',
            password='admin',
            host='192.168.86.36',
            port=1234)

        logger.info('Have connection')
        cur = conn.cursor()
        logger.info('Have cursor')

        for cricketStat in cricketStats:
            info_row = zelus_testing.get_info(cricketStat)
            match_registry = zelus_testing.get_registry(cricketStat)
            inning_data = zelus_testing.get_inning_data(cricketStat)
            zelus_testing.ingest_into_db(info_row, match_registry, inning_data, conn, cur, local_results_csv)