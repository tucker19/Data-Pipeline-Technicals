import unittest
import psycopg2
import pandas as pd

from dateutil.parser import parse
from fleetio_utils.scripts import Fleetio

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "1hxPf4fMbJevU47PfSX8O6_NVy081v8ninrH7K4ub6IA"
RANGE_NAME = "issues"
PUBLISH_LINK = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSK3cw1dAHgcrNna663hKuB_6_-u4PU4sPIA4vgokSarRmCRC9d1TvYQBA9mngYRycN_cAIfgpQUPa8/pub?output=csv"

class FleetioTest(unittest.Testcase):
	def test_1_db_connection(self):
		conn = psycopg2.connect(
			dbname='fleetio',
			user='admin',
			password='admin',
			host='192.168.86.36',
			port=1234)

	def test_2_ingest_from_gsheets_service_account(self):
		fleetio = Fleetio(SCOPES, SPREADSHEET_ID, RANGE_NAME, PUBLISH_LINK)
		df = fleetio.service_account_auth()
		self.assertEquals(len(df), 500, f'Should be 500 rows, not {len(df)} rows')

	def test_3_ingest_from_gsheets_published(self):
		fleetio = Fleetio(SCOPES, SPREADSHEET_ID, RANGE_NAME, PUBLISH_LINK)
		df = fleetio.published_csv()
		self.assertEquals(len(df), 500, f'Should be 500 rows, not {len(df)} rows')

	def test_4_clean_columns(self):
		fleetio = Fleetio(SCOPES, SPREADSHEET_ID, RANGE_NAME, PUBLISH_LINK)
		df = fleetio.service_account_auth()
		df = fleetio.update_col(df)
		columns = list(df.columns)

		self.assertEquals(len(columns), 14)
		self.assertEquals(columns[0], 'description', 'Column should be "description"')
		self.assertEquals(columns[1], 'created_by_id', 'Column should be "created_by_id"')
		self.assertEquals(columns[2], 'closed_by_id', 'Column should be "closed_by_id"')
		self.assertEquals(columns[3], 'vehicle_id', 'Column should be "vehicle_id"')
		self.assertEquals(columns[4], 'vehicle_meter_value', 'Column should be "vehicle_meter_value"')
		self.assertEquals(columns[5], 'vehicle_meter_unit', 'Column should be "vehicle_meter_unit"')
		self.assertEquals(columns[6], 'vehicle_meter_at', 'Column should be "vehicle_meter_at"')
		self.assertEquals(columns[7], 'custom_fields', 'Column should be "custom_fields"')
		self.assertEquals(columns[8], 'reported_at', 'Column should be "reported_at"')
		self.assertEquals(columns[9], 'resolved_at', 'Column should be "resolved_at"')
		self.assertEquals(columns[10], 'resolved_note', 'Column should be "resolved_note"')
		self.assertEquals(columns[11], 'state', 'Column should be "state"')
		self.assertEquals(columns[12], 'created_at', 'Column should be "created_at"')
		self.assertEquals(columns[13], 'updated_at', 'Column should be "updated_at"')

	def test_5_clean_data(self):
		fleetio = Fleetio(SCOPES, SPREADSHEET_ID, RANGE_NAME, PUBLISH_LINK)
		df = fleetio.service_account_auth()
		df = fleetio.update_col(df)
		main_df, custom_fields_df, main_list, custom_fields_list = fleetio.clean_data(df)

		self.assertEquals(len(main_list), 500, f'Should be 500 rows, not {len(df)} rows')

		first_row = main_list[0]
		self.assertEquals(len(first_row.keys()), 13, f'Should be 13 keys, not {len(first_row.keys())} keys')
		columns = ['description', 'created_by_id', 'closed_by_id', 'vehicle_id', 'vehicle_meter_value', 'vehicle_meter_unit', 'vehicle_meter_at', 'reported_at', 'resolved_at', 'resolved_note', 'state', 'created_at', 'updated_at']
		test_values = ['défaut frein parking', '6647665632626ec024efa63ffb2c8191', None,'c154a8db8182c5272d94914fcef8ef59', 408109.6749, 'km', parse('2023-07-03T00:00:00Z'), parse('2023-06-14T17:36:00Z'), parse('2023-06-15T13:08:49.155Z'), 'Ok', 'resolved', parse('2023-06-14T17:36:51.937Z'), parse('2023-06-15T13:08:49.201Z')]

		index = 0
		while index < len(columns):
			self.assertEquals(first_row.get(columns[index]), test_values[index], f'Column {columns[index]} do not match: {first_row.get(columns[index])} != {test_values[index]}')

		self.assertEquals(len(custom_fields_list), 105, f'Should be 105 rows, not {len(custom_fields_list)} rows')

if __main__ == '__main__':
	unittest.main()