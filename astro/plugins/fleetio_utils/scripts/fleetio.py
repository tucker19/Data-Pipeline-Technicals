import os.path
import pandas as pd
import gspread
#import pickle
import logging
import psycopg2

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as service_creds
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

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
			df = pd.DataFrame(values)
			df.columns = df.iloc[0]       

		return df

	def import_into_postrgres(self, conn, cur, df):
		logger.info('Possibily creating table...')
		cur.execute("""
			CREATE TABLE IF NOT EXISTS issues (
				description text,
				created_by_id text,
				closed_by_id text,
				vehicle_id text,
				vehicle_meter_value decimal,
				vehicle_meter_unit text,
				vehicle_meter_at timestamp,
				custom_fields json,
				reported_at timestamp,
				resolved_at timestamp,
				resolved_note text
				)
			""")

		logger.info('Ingesting into Postgres DB')
		df.to_sql('issues', conn, if_exists='append', index=False)