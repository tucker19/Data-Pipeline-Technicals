import asyncio
import logging
import time
import pandas as pd
import re
import sqlalchemy

from flipside import Flipside
from flipside.errors import QueryRunRateLimitError, QueryRunTimeoutError, QueryRunExecutionError, ServerError, ApiError, SDKError

logger = logging.getLogger()


async def get_query_from_files(file_path):
  """
    Method to gather query to be ran from a given file path
    Parameters:
      file_path (str) path for query file

    Returns:
      A dict with name and query from file
  """
  name = file_path.split('.')[0].split('/')[1]
  query = None

  with open(file_path) as queryFile:
    query = queryFile.read()

  return {'name': name, 'query': query}


async def process_query(flipside, name, query):
  """
    Method to run query with Flipside API, original method that has await key. This method should not be used, only kept for code process history
    Parameters:
      flipside (Flipside Object) Connection to Flipside API
      name (str) Name of query for logging purposes
      query (str) Query to run

    Returns:
      result (JSON) Object with query results
  """
  print(f'Running query:\n{query}')
  start = time.time()

  result = None
  try:
    result = flipside.query(query, timeout_minutes=10)
  except QueryRunRateLimitError as e:
    print(f"{name} query wasn rate limited: {e.message}")
  except QueryRunTimeoutError as e:
    print(f"{name} query has taken longer than 10 minutes to run: {e.message}")
  except QueryRunExecutionError as e:
    print(f"{name} query sql is malformed: {e.message}")
  except ServerError as e:
    print(f"A server-side error has occurred with {name} query: {e.message}")
  except ApiError as e:
    print(f"An api error has occurred with {name} query: {e.message}")
  except SDKError as e:
    print(
        f"A client-side SDK error has occurred with {name} query: {e.message}")

  print(f"{name} query ran in {time.time() - start} seconds")
  return result


async def alt_process_query(flipside, name, query):
  """
    Method to run query with Flipside API. This method properly uses await and AsyncIO. Can be used but what calls this will need to convert to Dataframe
    Parameters:
      flipside (Flipside Object) Connection to Flipside API
      name (str) Name of query for logging purposes
      query (str) Query to run

    Returns:
      result (JSON) Object with query results
  """
  print(f'Running query:\n{query}')
  start = time.time()
  result = None

  try:
    result = await asyncio.wait_for(asyncio.to_thread(flipside.query, query),
                                    timeout=10 * 60)
  except QueryRunRateLimitError as e:
    print(f"{name} query was rate limited: {e.message}")
  except asyncio.TimeoutError as e:
    print(f"{name} query has taken longer than 10 minutes to run: {e.message}")
  except QueryRunExecutionError as e:
    print(f"{name} query sql is malformed: {e.message}")
  except ServerError as e:
    print(f"A server-side error has occurred with {name} query: {e.message}")
  except ApiError as e:
    print(f"An api error has occurred with {name} query: {e.message}")
  except SDKError as e:
    print(
        f"A client-side SDK error has occurred with {name} query: {e.message}")

  print(f"{name} query ran in {time.time() - start} seconds")
  return result


async def df_process_query(flipside, name, query):
  """
    Method to run query with Flipside API. This method properly uses await and AsyncIO.
    Parameters:
      flipside (Flipside Object) Connection to Flipside API
      name (str) Name of query for logging purposes
      query (str) Query to run

    Returns:
      result (Pandas Dataframe) Dataframe containing query results
  """
  logger.info(f'Running query:\n{query}')
  start = time.time()
  result = None

  try:
    result = await asyncio.wait_for(asyncio.to_thread(flipside.query, query),
                                    timeout=10 * 60)
  except QueryRunRateLimitError as e:
    logger.error(f"{name} query was rate limited: {e.message}")
  except asyncio.TimeoutError as e:
    logger.error(f"{name} query has taken longer than 10 minutes to run: {e.message}")
  except QueryRunExecutionError as e:
    logger.error(f"{name} query sql is malformed: {e.message}")
  except ServerError as e:
    logger.error(f"A server-side error has occurred with {name} query: {e.message}")
  except ApiError as e:
    logger.error(f"An api error has occurred with {name} query: {e.message}")
  except SDKError as e:
    logger.error(
        f"A client-side SDK error has occurred with {name} query: {e.message}")

  logger.info(f"{name} query ran in {time.time() - start} seconds")
  records = result.records
  if not records:
    logger.error('No records found, was there a Validation thrown above? If Validation check if ran out of Query seconds')
    raise Exception("No records found")
  else:
    # print('We are looking for ts keys')
    """
    Noticed Dates and Timestamps were not getting auto detected and made type "object" in dataframes. 
    This piece of code is to find those columns and correct this. For these purposes any date or timestamp column has "_ts" at the end
    """
    ts_keys = []
    for key in records[0].keys():
      if re.search("_ts$", key):
        ts_keys.append(key)

    # print(f'ts keys {ts_keys}')
    result = pd.DataFrame(records)
    result.attrs['name'] = name
    for key in ts_keys:
      result[key] = pd.to_datetime(result[key])

  logger.info(result.to_markdown())
  return result


async def import_data_to_database(pg_hook, create_queries, df):
  """
    Method to take query results and insert into Postgres
    Parameters:
      pg_hook (Postgres Connector Hook) Connection to Postgres Database
      create_queries (List of Dict) List of possible create statements for query results
      df (Pandas Dataframe) Dataframe with query results

    Returns:
      None Data should be inserted into Postgres tables

    Note:
      This is designed to force a table to be created if one does not exist.
      As the environment I built this in Docker can be built and torn down fast we do not know if the destination tables are actually there.
      This si for safety purposes and eliminate checks looking for tables.
  """
  # print(df.atts['name'])
  df = df.drop(['__row_index'], axis=1, errors='ignore') #noticed an __row_number column, as this is unneeded removing from df
  logger.info(df.to_markdown())
  logger.info(df.dtypes)

  destination_table = None
  create_query_dict = create_queries.get(df.attrs['name'])
  if not create_query_dict:
    raise ValueError('Query name not present to have table be created')
  else:
    destination_table = create_query_dict.get('destination_table')
    create_query = create_query_dict.get('query')

  conn = pg_hook.get_conn()
  cur = conn.cursor()
  cur.execute(create_query)
  conn.commit()

  # Just showing progression of work with commented out code.
  # try:
  #   count = 0
  #   sql = None
  #   for dict_entry in dict_list:
  #     keys = ''
  #     values = ''
  #     for key in dict_entry.keys():
  #       value = dict_entry.get(key)
  #       if value:
  #         if not keys:
  #           keys += key
  #           values += f"'{value}'" if type(value) != float else value
  #         else:
  #           keys += f", {key}"
  #           values += f', {self.clean_value(value)}'
  #
  #     sql = f"INSERT INTO {table} ({keys}) VALUES ({values})"
  #     # print(sql)
  #     cur.execute(sql)
  #     conn.commit()
  #     count += 1
  #
  #   logger.info(f'Done importing {count} rows into {table}')
  # except Exception as e:
  #   logger.error(f"SQL: {sql}")
  #   raise e

  #pg_hook.insert_rows(destination_table, df)
  df.to_sql(name=destination_table, con=pg_hook.get_sqlalchemy_engine(), if_exists='append', index=False)
