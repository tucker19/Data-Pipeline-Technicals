import os.path
import pandas as pd
import gspread
#import pickle
import logging
import psycopg2
import json
import numpy as np

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as service_creds
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dateutil.parser import parse

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SAMPLE_SPREADSHEET_ID = "1hxPf4fMbJevU47PfSX8O6_NVy081v8ninrH7K4ub6IA"
SAMPLE_RANGE_NAME = "issues"
PUBLISH_LINK = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSK3cw1dAHgcrNna663hKuB_6_-u4PU4sPIA4vgokSarRmCRC9d1TvYQBA9mngYRycN_cAIfgpQUPa8/pub?output=csv"
logger = logging.getLogger()

class Fleetio():
	def __init__(self, scopes=None, spreadsheet_id=None, range_name=None, publish_link=None):
		self.scopes = scopes
		self.spreadsheet_id = spreadsheet_id
		self.range_name = range_name
		self.publish_link = publish_link
		
	#Spreadsheet has to be published to use this and formatted as a csv
	def published_csv(self):
		return pd.read_csv(self.publish_link)

	#User account Auth but could never get working
	# def user_auth(self):
	#   	creds = None
	#   	df = None

	#   	if os.path.exists('token.pickle'):
	#       	with open('token.pickle', 'rb') as token:
	#           	creds = pickle.load(token)

	#   	if not creds or not creds.valid:
	#       	if creds and creds.expired and creds.refresh_token:
	#           	creds.refresh(Request())
	#       	else:
	#           	flow = InstalledAppFlow.from_client_secrets_file('secrets/fleetio_creds.json', self.scopes)
	#           	creds = flow.run_local_server(port=0)

	#       	# Save the credentials for the next run
	#       	with open('token.pickle', 'wb') as token:
	#           	pickle.dump(creds, token)

	#   	service = build('sheets', 'v4', credentials=creds)

	#   	# Call the Sheets API
	#   	sheet = service.spreadsheets()
	#   	result = sheet.values().get(spreadsheetId=self.spreadsheet_id, range=self.range_name).execute()
	#   	values = result.get('values', [])
	#   	if not values:
	#       	logger.error('No data found.')
	#   	else:
	#       	df = pd.DataFrame(values)
	#       	df.columns = df.iloc[0]
	      	
	#     return df

	#Currently working method
	def service_account_auth(self, tab_index=0):
		df = None

		# Read the .json file and authenticate with the links
		credentials = service_creds.from_service_account_file('secrets/service_account.json', scopes=self.scopes)

		# Request authorization and open the selected spreadsheet
		gc = gspread.authorize(credentials).open_by_key(self.spreadsheet_id)

		# Prompts for all spreadsheet values
		values = gc.get_worksheet(tab_index).get_all_values()

		if not values:
			logger.error('No data found.')
		else:
			# Turns the return into a dataframe
			logger.info('Data Found')
			df = pd.DataFrame(values)
			df.columns = df.iloc[0]       

		return df

	def import_into_postrgres(self, conn, cur, df, table, execute_str):
		logger.info('Possibily creating table...')
		cur.execute(execute_str)

		logger.info('Ingesting into Postgres DB')
		df.to_sql(name=table, con=conn, if_exists='append', index=False)
		logger.info(f'Done importing {len(df.index)} rows into {table}')

	def import_into_postrgres_sql(self, conn, cur, dict_list, table, execute_str):
		logger.info(f'Possibily creating table {table}...')
		logger.info(f"Creation String:\n{execute_str}")
		cur.execute(execute_str)

		logger.info('Ingesting into Postgres DB')
		try:
			count = 0
			sql = None
			for dict_entry in dict_list:
				keys = ''
				values = ''
				for key in dict_entry.keys():
					value = dict_entry.get(key)
					if value:
						if not keys:
							keys += key
							values += f"'{value}'" if type(value) != float else value
						else:
							keys += f", {key}"
							values += f', {self.clean_value(value)}'

				sql = f"INSERT INTO {table} ({keys}) VALUES ({values})"
				#print(sql)
				cur.execute(sql)
				conn.commit()
				count += 1

			logger.info(f'Done importing {count} rows into {table}')
		except Exception as e:
			logger.error(f"SQL: {sql}")
			raise e

		return conn, cur


	def update_col(self, df):
		cols = list(df.columns)
		new_cols = {}

		for col in cols:
			new_cols[col] = col.lower()

		df = df.rename(columns=new_cols)
		return df

	def clean_data(self, df):
		records = df.to_dict('records')
		#print(df_dict)
		#print(df_dict.keys())
		#date_obj = datetime.strptime(date_str, "%b %d %Y %I:%M%p")
		#2023-07-04T00:00:00Z

		temp = []
		custom_fields_list = []
		for record in records[1:]:
			#print(f'Record: {record}')
			#print()
			temp_dict = {}
			try:
				description = None if not record.get('description').strip() else record.get('description')
				temp_dict['description'] = None if not description else description.replace("'", "")
				temp_dict['created_by_id'] = None if not record.get('created_by_id').strip() else record.get('created_by_id')
				temp_dict['closed_by_id'] = None if not record.get('closed_by_id').strip() else record.get('closed_by_id')
				vehicle_id = None if not record.get('vehicle_id').strip() else record.get('vehicle_id')
				temp_dict['vehicle_id'] = vehicle_id
				temp_dict['vehicle_meter_value'] = None if not record.get('vehicle_meter_value').strip() else abs(float(record.get('vehicle_meter_value')))
				temp_dict['vehicle_meter_unit'] = None if not record.get('vehicle_meter_unit').strip() else record.get('vehicle_meter_unit')
				temp_dict['vehicle_meter_at'] = None if not record.get('vehicle_meter_at').strip() else parse(record.get('vehicle_meter_at'))
				#temp_dict['custom_fields'] = None if '{' not in record.get('custom_fields') else json.loads(record.get('custom_fields'))
				temp_dict['reported_at'] = None if not record.get('reported_at').strip() else parse(record.get('reported_at'))
				temp_dict['resolved_at'] = None if not record.get('resolved_at').strip() else parse(record.get('resolved_at'))
				temp_dict['resolved_note'] = None if not record.get('resolved_note').strip() else record.get('resolved_note')
				temp_dict['state'] = None if not record.get('state').strip() else record.get('state')
				temp_dict['created_at'] = None if not record.get('created_at').strip() else parse(record.get('created_at'))
				temp_dict['updated_at'] = None if not record.get('updated_at').strip() else parse(record.get('updated_at'))
				#print(temp_dict)
				#print('----------')
				temp.append(temp_dict)

				custom_field = None if '{' not in record.get('custom_fields') else json.loads(record.get('custom_fields'))
				if custom_field:
					for key in custom_field.keys():
						custom_fields_dict = {}
						custom_fields_dict['vehicle_id'] = vehicle_id
						custom_fields_dict['key'] = key
						custom_fields_dict['value'] = custom_field.get(key)
						custom_fields_list.append(custom_fields_dict)
			except Exception as e:
				logger.error(record)
				raise e

		main_df = pd.DataFrame(temp)
		custom_fields_df = pd.DataFrame(custom_fields_list)
		# print(custom_fields_list)
		# print(df.dtypes)
		# print(df)
		#print(custom_fields_df)
		return main_df, custom_fields_df, temp, custom_fields_list

	def clean_value(self, value):
		result = None
		if type(value) == float:
			result = value
		elif type(value) == dict:
			result = f'"{value}"'
		else:
			result = f"'{value}'"

		return result