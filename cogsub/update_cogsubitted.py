from __future__ import print_function
import pickle
import os.path
import time
import argparse
import os
import requests
import meta
import sys
import json 
import logging
import pprint
from marshmallow import EXCLUDE
from climbfiles import ClimbFiles
import csv 
from majora_util import majora_request
from collections import Counter
import re 
from cogschemas import Cogmeta, RunMeta, LibraryBiosampleMeta, LibraryHeaderMeta
import requests
import json
from cogschemas import Cogmeta
import pprint
import logging
from marshmallow import EXCLUDE
from requests_oauthlib import OAuth2Session
import os 
import sys 
from majora_endpoints import ENDPOINTS
from datetime import datetime 
from majora_util import majora_oauth

from oauth2client.service_account import ServiceAccountCredentials
import gspread

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

all_submitted = {} 

for sheet_name in ["SARSCOV2-REACT-Metadata", 'SARCOV2-Metadata']:
    majora_token = "majora.json"
    config = load_config(majora_token)
    majora_username = config['majora_username']
    credentials='credentials.json'
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials, scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name).sheet1
    column_position = sheet.row_values(1)
    if sheet_name == "SARSCOV2-REACT-Metadata":
        row_position = sheet.col_values(3)        
    else:
        row_position = sheet.col_values(1)    
    all_values = sheet.get_all_records()
    cells_to_update = []

    for x in all_values:
        sample_name = x.get('central_sample_id')
        if sample_name:
            results = majora_sample_exists(sample_name, majora_username, '99f50e3d-75a3-4f96-9dac-7bb67c6be9b1', config['majora_server'], False)
            cog_value = ""
            if results:
                cog_value = "YES"
                all_submitted[results[0]] = results[0]
            cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('is_submitted_to_cog')+1, value=cog_value))            

    if cells_to_update:
        print('Updating values')
        sheet.update_cells(cells_to_update)   
print(f'central_sample_id\tbiosample_id')
for x,y  in all_submitted.items(): 
    print(f'{x}\t{y}')
