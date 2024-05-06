from zelus import ZelusCricket
import glob
from airflow.models import BaseOperator
import logging
import psycopg2

logger = logging.getLogger()

class ZelusOperator(BaseOperator):
    def __init__(self, continue_run, local_save, limit_ingestion, dbname, user, password, host, port, *args, **kwargs):
        self.continue_run = continue_run
        self.local_save = local_save
        self.limit_ingestion = limit_ingestion
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        super(ZelusOperator, self).__init__(*args, **kwargs)

    def execute(self, context=None):
        zelus_testing = ZelusCricket()
        todays_date_str = zelus_testing.todays_date_dir()
        zelus_testing.get_data_from_site(todays_date_str)
        # zelus_testing.get_data_from_local(todays_date_str)

        jsonFiles = glob.glob(f"data/{todays_date_str}/*.json")
        jsonFiles.sort()

        local_results_csv = [zelus_testing.get_result_destination(todays_date_str, key) for key in ['info', 'match_registry', 'innings']] if self.local_save else {}
        conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port)

        logger.info('Have connection')
        cur = conn.cursor()
        logger.info('Have cursor')

        successful_files = zelus_testing.get_successful_rows(conn, cur) if self.continue_run else []
        madatory_files = ['1000851', '1000885', '1003271', '1003887', '1130661', '1160280', '1188427', '1202249']

        count = 1
        num_of_files = len(jsonFiles)
        # for cricketStat in cricketStats:
        for jsonFilePath in jsonFiles:
            logger.info(f'Processing "{jsonFilePath}", {count} of {num_of_files} files')
            cricketStat = zelus_testing.get_data_from_json_file(todays_date_str, jsonFilePath)
            filename = cricketStat.get('filename')
            if self.check_if_we_limit(self.limit_ingestion, madatory_files, count, filename):
                if filename in successful_files:
                    logger.info(f'File {filename}.csv has already successful been processed')
                else:
                    info_row = zelus_testing.get_info(cricketStat)
                    match_registry = zelus_testing.get_registry(cricketStat)
                    inning_data = zelus_testing.get_inning_data(cricketStat)
                    conn, cur = zelus_testing.ingest_into_db(info_row, match_registry, inning_data, conn, cur, local_results_csv)
                    logger.info(f'Finished processing {filename}.csv')

            count += 1

        cur.close()
        conn.close()

        zelus_testing.delete_files(todays_date_str)

    def check_if_we_limit(self, limit_ingestion, madatory_files, count, filename):
        result = True
        if limit_ingestion:
            if count > 1000 and filename not in madatory_files:
                result = False

        return result
