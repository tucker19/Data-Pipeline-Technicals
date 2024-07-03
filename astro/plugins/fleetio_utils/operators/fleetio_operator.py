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
		#df = fleetio.published_csv()
		df = fleetio.service_account_auth()
		df = fleetio.update_col(df)
		main_df, custom_fields_df, main_list, custom_fields_list = fleetio.clean_data(df)

		logger.info('Connection to DB')
		pg_hook = PostgresHook.get_hook(conn_id='technicals_postgres')

		conn = pg_hook.get_conn()
		# conn = psycopg2.connect(
		# 	dbname='technicals',
		# 	user='admin',
		# 	password='admin',
		# 	host='192.168.86.36',
		# 	port=1234)

		logger.info('Have connection')
		cur = conn.cursor()
		logger.info('Have cursor')

		#custom_fields JSONB DEFAULT NULL,
		issues_execute_str = """
		CREATE TABLE IF NOT EXISTS issues (
			description text DEFAULT NULL,
			created_by_id text DEFAULT NULL,
			closed_by_id text DEFAULT NULL,
			vehicle_id text DEFAULT NULL,
			vehicle_meter_value decimal DEFAULT NULL,
			vehicle_meter_unit text DEFAULT NULL,
			vehicle_meter_at timestamp DEFAULT NULL,
			reported_at timestamp DEFAULT NULL,
			resolved_at timestamp DEFAULT NULL,
			resolved_note text DEFAULT NULL,
			state text DEFAULT NULL,
			created_at timestamp DEFAULT NULL,
			updated_at timestamp DEFAULT NULL);
		"""
		conn, cur = fleetio.import_into_postrgres_sql(conn, cur, main_list, 'issues', issues_execute_str)

		custom_fields_execute_str = """
		CREATE TABLE IF NOT EXISTS custom_fields (
			vehicle_id text DEFAULT NULL,
			key text DEFAULT NULL,
			value text DEFAULT NULL);
		"""
		conn, cur = fleetio.import_into_postrgres_sql(conn, cur, custom_fields_list, 'custom_fields', custom_fields_execute_str)

		cur.close()
		conn.close()
