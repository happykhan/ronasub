"""
check_meta checks if samples are uploaded to CLIMB and if the metadata is the same. 

Requires login for google sheets

### CHANGE LOG ### 
2021-04-19 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build - split from cogsub 
"""
import logging 
import json 
import requests
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from marshmallow import EXCLUDE

def add_missing_rows(submission_sheet_name, gcredentials): 
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(gcredentials, scope)
    client = gspread.authorize(creds)    
    submission_sheet = client.open(submission_sheet_name).sheet1
    for sheet_name in ["SARSCOV2-REACT-Metadata", 'SARCOV2-Metadata']:
        submission_values = submission_sheet.col_values(1)
        sheet = client.open(sheet_name).sheet1
        all_values = sheet.get_all_records()
        sample_list = [] 
        for row in all_values:
            if row['central_sample_id'] not in submission_values:
                if row['central_sample_id'] and row['run_name']:
                    new_row = [row['central_sample_id'], row['library_name'], row['run_name']] 
                    sample_list.append(new_row)
        if sample_list:
            submission_sheet.resize(len(submission_values))
            submission_sheet.append_rows(sample_list)



def check_meta(majora_token, sheet_name, submission_sheet_name, gcredentials):
   
    all_submitted = {} 

    add_missing_rows(submission_sheet_name, gcredentials)

    majora_token = "majora.json"
    config = load_config(majora_token)
    majora_username = config['majora_username']
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(gcredentials, scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name).sheet1

    all_values = sheet.get_all_records()
    cells_to_update = []

    submission_sheet = client.open(submission_sheet_name).sheet1
    column_position = submission_sheet.row_values(1)
    row_position = submission_sheet.col_values(1)

    for x in all_values:
        sample_name = x.get('central_sample_id')
        if sample_name:
            if sample_name in row_position:
                results = majora_sample_exists(sample_name, majora_username, config['majora_token'], config['majora_server'], False)
                cog_value = ""
                if results:
                    cog_value = "YES"
                    all_submitted[results[0]] = results[0]
                
                cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('is_submitted_to_cog')+1, value=cog_value))            

    if cells_to_update:
        print('Updating values')
        submission_sheet.update_cells(cells_to_update)   
    print(f'central_sample_id\tbiosample_id')
    for x,y  in all_submitted.items(): 
        print(f'{x}\t{y}')
       


def load_config(config="majora.json"):
    config_dict = json.load(open(config))
    return config_dict

def majora_sample_exists(sample_name, username, key, SERVER, dry = False):
    address = SERVER + '/api/v2/artifact/biosample/get/'
    payload = dict(central_sample_id=sample_name, username=username, token=key, client_name='cogsub', client_version='0.1')
    response = requests.post(address, headers = {"Content-Type": "application/json", "charset": "UTF-8"}, json = payload)
    try:
        response_dict = json.loads(response.content)       
        if response_dict['errors'] == 0:
            central_sample_id = list(response_dict['get'].keys())[0] 
            if not list(response_dict['get'].values())[0].get('biosample_sources'):
                print(sample_name + ' has no biosample ')
                return False
            biosample_source = list(response_dict['get'].values())[0]['biosample_sources'][0]['biosample_source_id']            
            return [central_sample_id, biosample_source]
        else:
            print(sample_name + ' appears to be missing ')
            return False
    except json.decoder.JSONDecodeError:
        logging.error(response)
        return False

