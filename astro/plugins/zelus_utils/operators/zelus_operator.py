from zelus import ZelusCricket
import glob
from airflow.models import BaseOperator
import logging
import psycopg2

logger = logging.getLogger()

class ZelusOperator(BaseOperator):
    def __init__(self, dbname, *args, **kwargs):
        self.dbname = dbname
        super(ZelusOperator, self).__init__(*args, **kwargs)

    def execute(self, context=None):
        zelus_testing = ZelusCricket()
        todays_date_str = zelus_testing.todays_date_dir()
        zelus_testing.get_data_from_site(todays_date_str)
        # zelus_testing.get_data_from_local(todays_date_str)

        jsonFiles = glob.glob(f"data/{todays_date_str}/*.json")
        jsonFiles.sort()
        #logger.info(f'jsonFiles:\n{jsonFiles}')
        # cricketStats = []
        # input_count = 0
        # for jsonFilePath in jsonFiles:
        #     input_count += 1
        #     cricketStats.append(zelus_testing.get_data_from_json_file(todays_date_str, jsonFilePath))
        #     # if input_count > 1:
        #     #     break

        local_results_csv = {}
        # for key in ['info', 'match_registry', 'innings']:
        #     local_results_csv[key] = zelus_testing.get_result_destination(todays_date_str, key)

        conn = psycopg2.connect(
            dbname=self.dbname,
            user='admin',
            password='admin',
            host='192.168.86.36',
            port=1234)

        logger.info('Have connection')
        cur = conn.cursor()
        logger.info('Have cursor')

        successful_files = []#zelus_testing.get_successful_rows(conn, cur)
        madatory_files = ['1000851', '1000885', '1003271', '1003887', '1130661', '1160280', '1188427', '1202249']

        count = 1
        num_of_files = len(jsonFiles)
        # for cricketStat in cricketStats:
        for jsonFilePath in jsonFiles:
            logger.info(f'Processing "{jsonFilePath}", {count} of {num_of_files} files')
            cricketStat = zelus_testing.get_data_from_json_file(todays_date_str, jsonFilePath)
            filename = cricketStat.get('filename')
            if count <= 1000 or filename in madatory_files:
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