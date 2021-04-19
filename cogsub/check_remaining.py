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

def load_config(config="majora.json"):
    config_dict = json.load(open(config))
    return config_dict

def majora_sample_status(sample_name, username, key, SERVER, dry = False):
    address = SERVER + '/api/v2/artifact/biosample/get/'
    payload = dict(central_sample_id=sample_name, username=username, token=key, client_name='cogsub', client_version='0.1')
    response = requests.post(address, headers = {"Content-Type": "application/json", "charset": "UTF-8"}, json = payload)
    try:
        response_dict = json.loads(response.content)       
        if response_dict['errors'] == 0:
            central_sample_id = list(response_dict['get'].keys())[0] 
            if list(response_dict['get'].values())[0].get('published_as'):
                return 'WARNING: Sample already has sequence!'
            else:
                return 'Sample ready for sequencing'
        elif response_dict['messages'][0].startswith('\'NoneType\''):
            return 'No sample metadata submitted'
        else:
            return 'ERROR Fetching record'
    except json.decoder.JSONDecodeError:
        logging.error(response)
        return 'ERROR fetching record'

import csv 

with open('remaining_sanger') as j:
    majora_token = "majora.json"
    config = load_config(majora_token)
    majora_username = config['majora_username']
    out = open('sanger_checklist', 'w')
    out.write(f'ID\tPLATE\tSUBMITTED\n')
    for row in csv.DictReader(j, dialect=csv.excel_tab):
        sample_name = row.get('ID')
        plate_id = row.get('PLATE')
        status = majora_sample_status(sample_name, majora_username, '4a7a676c-3936-4248-8944-d837a80cfc41', config['majora_server'], False)
        out.write(f'{sample_name}\t{plate_id}\t{status}\n')
