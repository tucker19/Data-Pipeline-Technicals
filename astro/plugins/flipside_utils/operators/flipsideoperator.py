import asyncio
import logging
import time

from airflow.models import BaseOperator
from flipside import Flipside
from flipsideutils import df_process_query, alt_process_query, get_query_from_files, import_data_to_database
from create_queries import CREATE_QUERIES
from dag_queries import QUERIES
from airflow.providers.postgres.hooks.postgres import PostgresHook

# cda03c7c-8a48-4e1f-98e7-130defa3d39c	alt_account
FLIPSIDE_CRYPTO_API = 'cda03c7c-8a48-4e1f-98e7-130defa3d39c'

QUERY_FILES = [ # originally was doing files with queries but decided to just make `dag_queries` with list of dict
  # 'sql/Top 10 Senders by amount in last hour.sql',
  # 'sql/Top 10 Receivers by amount in last hour.sql',
  # 'sql/Top 10 Senders for a block_id by transfers.sql',
  # 'sql/Top 10 Receivers for a block_id by transfers.sql',
  'sql/Number of Transactions per Day over last week.sql'  # ,
  # 'sql/Number of Transactions per Month last 6 month.sql'
]

logger = logging.getLogger()


class FlipsideOperator(BaseOperator):

  def __init__(self, *args, **kwargs):
    self.flipside = Flipside(FLIPSIDE_CRYPTO_API, "https://api-v2.flipsidecrypto.xyz")
    self.queries = QUERIES
    self.create_queries = CREATE_QUERIES

    super(FlipsideOperator, self).__init__(*args, **kwargs)

  async def async_execute_flipside(self):
    """
    Method to using AsyncIO to run multiple queries in parallel
    Parameters:
      None

    Returns:
      List of Dataframes
    """
    # result_dfs = []
    start_time = time.time()

    tasks = [
      df_process_query(self.flipside, query_record.get('name'),
                       query_record.get('query'))
      for query_record in self.queries
    ]

    # Run tasks concurrently and gather results
    results = await asyncio.gather(*tasks)
    logger.info(f"All queries ran in {time.time() - start_time} seconds")

    # # Process results
    # for result in results:
    #   records = result.records
    #   if not records:
    #     print('No records found')
    #     #raise Exception("No records found")
    #   else:
    #     result_dfs.append(pd.DataFrame(records))

    return results

  async def get_queries(self):
    """
    Method to gather all queries to be ran from a given list of file paths
    Parameters:
      None

    Returns:
      A list of dict with name and query from file
    """
    tasks = [get_query_from_files(query_file) for query_file in QUERY_FILES]
    return await asyncio.gather(*tasks)

  async def execute_ingestion(self, dfs):
    """
    Method to connect to Postgres DB, create tables and insert Dataframes into those tables
    Parameters:
      dfs (List of DataFrames)

    Returns:
      None
    """
    logger.info('Connection to DB')
    pg_hook = PostgresHook.get_hook(conn_id='technicals_postgres')

    tasks = [
      import_data_to_database(pg_hook, self.create_queries, df)
      for df in dfs
    ]

    await asyncio.gather(*tasks)

  def execute(self, context=None):
    """
        Method to gather all queries to be ran from a given list of file paths
        Parameters:
          None

        Returns:
          A list of dict with name and query from file
    """
    logger.info(f'Gathering data on {len(QUERY_FILES)} query files')
    # query_records = asyncio.run(self.get_queries())
    # # print(f'We found {len(query_records)} queries')
    # self.queries = query_records

    # dfs = []
    # if not query_records:
    #   print('Something is wrong, no queries found')
    # else:
    #   dfs = asyncio.run(self.async_execute_flipside())
    dfs = asyncio.run(self.async_execute_flipside())

    # Create and ingest
    asyncio.run(self.execute_ingestion(dfs))


