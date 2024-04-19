from airflow.models import BaseOperator
from fleetio_utils.scripts.fleetio import Fleetio
import logging
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.connection import Connection

logger = logging.getLogger()

class FleetioOperator(BaseOperator):
	def __init__(self, scopes=None, spreadsheet_id=None, range_name=None, publish_link=None, *args, **kwargs):
		self.scopes = scopes
		self.spreadsheet_id = spreadsheet_id
		self.range_name = range_name
		self.publish_link = publish_link

		super(FleetioOperator, self).__init__(*args, **kwargs)

	def execute(self, context=None):
		logger.info('Executing code...')
		fleetio = Fleetio(self.scopes, self.spreadsheet_id, self.range_name, self.publish_link)
		#df = fleetio.published_csv()
		logger.info('Getting df')
		df = fleetio.service_account_auth()

		logger.info('Connection to DB')
		pg_hook = PostgresHook.get_hook('fleetio_db')
		conn = pg_hook.get_conn()
		# conn = psycopg2.connect(
		# 	dbname='fleetio',
		# 	user='admin',
		# 	password='admin',
		# 	host='localhost',
		# 	port=1234)

		logger.info('Have connection')
		cur = conn.cursor()
		logger.info('Have cursor')

		fleetio.import_into_postrgres(conn, cur, df)

		cur.close()
		conn.close()
