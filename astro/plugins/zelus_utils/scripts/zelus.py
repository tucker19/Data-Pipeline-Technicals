import zipfile
import datetime
import os
import urllib.request
import json
import re
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger()

class ZelusCricket():
    def __init__(self):
        pass

    def todays_date_dir(self):
        todays_date = datetime.datetime.now()
        todays_date_str = todays_date.strftime('%Y%m%d_%H')
        if not os.path.exists(f'data/{todays_date_str}'):
            os.makedirs(f'data/{todays_date_str}')

        logger.info(f'Todays date directory is {todays_date_str}')
        return todays_date_str

    def get_result_destination(self, todays_date_str, filename):
        if not os.path.exists(f'results/{todays_date_str}'):
            os.makedirs(f'results/{todays_date_str}')

        csv_file = Path(f'results/{todays_date_str}/{filename}.csv')
        if csv_file.exists():
            dup_count = 1
            while csv_file.exists():
                csv_file = Path(f'results/{todays_date_str}/{filename}_{dup_count}.csv')

        return csv_file

    def get_data_from_site(self, todays_date_str):
        logger.info('Getting JSON Archive')
        urllib.request.urlretrieve("https://cricsheet.org/downloads/all_json.zip",
                                   f"data/{todays_date_str}/all_json.zip")
        logger.info('Finished getting JSON Archive')

        if len(os.listdir(f'data/{todays_date_str}')) == 1:
            logger.info('Exctracting JSON Files')
            with zipfile.ZipFile(f'data/{todays_date_str}/all_json.zip', 'r') as zip_ref:
                zip_ref.extractall(f'data/{todays_date_str}')

            logger.info('Successfully extracted JSON files from zip')

        else:
            logger.info('Directory already has json files, skipping extraction')

    def get_data_from_local(self, todays_date_str):
        with zipfile.ZipFile('data/cricsheet_all_json.zip', 'r') as zip_ref:
            zip_ref.extractall(f'data/{todays_date_str}')

    def get_data_from_json_file(self, todays_date_str, filePath):
        json_dict = {}
        # print(f'File Path pass: "{filePath}"')
        filename = re.search(r"data/" + todays_date_str + r"/(.*)\.json", filePath).group(1)
        logger.info(f'Adding filename {filename} to JSON Object')
        with open(filePath) as jsonFile:
            json_dict = json.load(jsonFile)
            json_dict['filename'] = filename

        return json_dict

    def get_registry(self, json_dict):
        logger.info('Getting Registry list')
        filename = json_dict.get('filename')
        info_section = json_dict.get('info')

        players = info_section.get('players')
        match_registry = []
        registry_people = info_section.get('registry').get('people')
        for name in registry_people:
            # print(f'Name: {name}')
            match_registry_dict = {}
            match_registry_dict['filename'] = filename
            match_registry_dict['name'] = name
            match_registry_dict['id'] = registry_people.get(name)

            for team in players:
                team_players = players.get(team)
                if name in team_players:
                    match_registry_dict['team'] = team

            match_registry.append(match_registry_dict)
        return match_registry

    def get_info(self, json_dict):
        logger.info('Getting Match Info')
        filename = json_dict.get('filename')
        info_section = json_dict.get('info')

        info_row = {}
        info_row['filename'] = filename
        info_row['balls_per_over'] = info_section.get('balls_per_over')
        info_row['city'] = info_section.get('city')
        info_row['dates'] = '/'.join(info_section.get('dates'))
        info_row['match_number'] = info_section.get('match_number')
        info_row['match_name'] = info_section.get('name')
        info_row['gender'] = info_section.get('gender')
        info_row['match_type'] = info_section.get('match_type')
        info_row['match_type_number'] = info_section.get('match_type_number')
        info_row['match_referees'] = ','.join(info_section.get('officials').get('match_referees'))
        info_row['reserve_umpires'] = ','.join(info_section.get('officials').get('reserve_umpires'))
        info_row['tv_umpires'] = ','.join(info_section.get('officials').get('tv_umpires'))
        info_row['umpires'] = ','.join(info_section.get('officials').get('umpires'))
        info_row['total_runs'] = info_section.get('outcome').get('by').get('runs')
        info_row['winner'] = info_section.get('outcome').get('winner')
        info_row['player_of_match'] = ','.join(info_section.get('player_of_match'))
        info_row['season'] = info_section.get('season')
        info_row['team_type'] = info_section.get('team_type')
        info_row['toss_decision'] = info_section.get('toss').get('decision')
        info_row['toss_winner'] = info_section.get('toss').get('winner')
        info_row['venue'] = info_section.get('venue')

        return info_row

    def get_inning_data(self, json_dict):
        logger.info('Getting Inning Data list')
        filename = json_dict.get('filename')
        innings_section_list = json_dict.get('innings')
        innings_rows = []
        count = 0
        for innings_section in innings_section_list:
            count += 1
            team = innings_section.get('team')
            overs = innings_section.get('overs')
            for over in overs:
                over_num = over.get('over')
                for delivery in over.get('deliveries'):
                    over_deliveries_row = {}
                    over_deliveries_row['filename'] = filename
                    over_deliveries_row['team'] = team
                    over_deliveries_row['inning'] = count
                    over_deliveries_row['over'] = over_num
                    over_deliveries_row['batter'] = delivery.get('batter')
                    over_deliveries_row['bowler'] = delivery.get('bowler')
                    over_deliveries_row['non_striker'] = delivery.get('non_striker')
                    over_deliveries_row['runs_batter'] = delivery.get('runs').get('batter')
                    over_deliveries_row['runs_extras'] = delivery.get('runs').get('extras')
                    over_deliveries_row['run_total'] = delivery.get('runs').get('total')

                    if 'wickets' in delivery.keys():
                        for wicket in delivery.get('wickets'):
                            wicket_row = over_deliveries_row
                            wicket_row['kind'] = wicket.get('kind')
                            wicket_row['player_out'] = wicket.get('player_out')
                            fielders = ''
                            fielders_list = wicket.get('fielders') if wicket.get('fielders') else []
                            for fielder in fielders_list:
                                fielder_name = fielder.get('name')
                                if not fielders:
                                    fielders += fielder_name
                                else:
                                    fielders += '/' + fielder_name

                            wicket_row['fielders'] = fielders if fielders else None
                            innings_rows.append(wicket_row)
                    else:
                        over_deliveries_row['kind'] = None
                        over_deliveries_row['player_out'] = None
                        over_deliveries_row['fielders'] = None
                        innings_rows.append(over_deliveries_row)

        return innings_rows

    def ingest_into_db(self, info_row, match_registry, innings_rows, conn, cur, local_results_csv=None):
        logger.info('Ingesting')
        # print(f'info_row:\n{info_row}')

        # print('----------')
        # print('match_registry:')
        # for registry in match_registry:
        #   print(registry)

        # print('----------')
        # print('Play by Play?')
        # for innings_row in innings_rows:
        #   inning  = innings_row.get('inning')
        #   over = innings_row.get('over')
        #   print(f'Inning-{inning}, Over-{over}:{innings_row}')

        if local_results_csv:
            # print('Found local save paths')
            # print('----------')
            # print(info_row)
            # print('----------')
            # df = pd.DataFrame([info_row])
            self.output_csv([info_row], local_results_csv.get('info'))
            self.output_csv(match_registry, local_results_csv.get('match_registry'))
            self.output_csv(innings_rows, local_results_csv.get('innings'))
        else:
            match_info_execute_str = """
            CREATE TABLE IF NOT EXISTS match_info (
            filename text DEFAULT NULL,
            balls_per_over numeric DEFAULT 0,
            city text DEFAULT NULL,
            dates text DEFAULT NULL,
            match_number numeric DEFAULT 0,
            match_name text DEFAULT NULL,
            gender text DEFAULT NULL,
            match_type text DEFAULT NULL,
            match_type_number numeric DEFAULT 0,
            match_referees text DEFAULT NULL,
            reserve_umpires text DEFAULT NULL,
            tv_umpires text DEFAULT NULL,
            umpires text DEFAULT NULL,
            total_runs numeric DEFAULT 0,
            winner text DEFAULT NULL,
            player_of_match text DEFAULT NULL,
            season text DEFAULT NULL,
            team_type text DEFAULT NULL,
            toss_decision text DEFAULT NULL,
            toss_winner text DEFAULT NULL,
            venue text DEFAULT NULL);
            """
            print('Something messed up')

    def output_csv(self, table_rows, csv_file):
        file_exists = True
        if not csv_file.exists():
            file_exists = False
            logger.info(f'File {csv_file} does not exists so header write needed')
        else:
            logger.info(f'Appending to {csv_file}')

        df = pd.DataFrame(table_rows)
        if not file_exists:
            df.to_csv(csv_file, index=False)
        else:
            df.to_csv(csv_file, mode='a', header=False, index=False)
        # for table_row in tabe_rows:
        #   df = pd.DataFrame(table_row)
        #   if not file_exists:
        #     df.to_csv(csv_file)
        #   else:
        #     df.to_csv(csv_file, mode='a', header=False)

        # keys = tabe_rows[0].keys()
        # with open(csv_file, 'w', newline='') as file:
        #   writer = csv.DictWriter(file, fieldnames=keys)
        #   if not file_exists:
        #     writer.writeheader()

        #   for table_row in tabe_rows:
        #     writer.writerow(table_row)
