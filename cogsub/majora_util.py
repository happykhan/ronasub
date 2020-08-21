import requests
import json
from cogschemas import Cogmeta
import pprint
import logging
from marshmallow import EXCLUDE

def majora_sample_exists(sample_name, username, key, SERVER, dry = False):
    address = SERVER + '/api/v2/artifact/biosample/get/'
    payload = dict(central_sample_id=sample_name, username=username, token=key, client_name='cogsub', client_version='0.1')
    response = requests.post(address, headers = {"Content-Type": "application/json", "charset": "UTF-8"}, json = payload)
    if not dry:
        try:
            response_dict = json.loads(response.content)
            if response_dict['errors'] == 0:
                return True
            else:
                return False
        except json.decoder.JSONDecodeError:
            logging.error(response)
            return False
    else:
        logging.debug(pprint.pprint(payload))
        return True

def majora_is_dirty_sample(sample, username, key, SERVER, dry = True):
    address = SERVER + '/api/v2/artifact/biosample/get/'
    sample = Cogmeta().load(sample)
    cog_id = sample['central_sample_id']
    payload = dict(central_sample_id=cog_id, username=username, token=key, client_name='cogsub', client_version='0.1')
    response = requests.post(address, headers = {"Content-Type": "application/json", "charset": "UTF-8"}, json = payload)
    response_dict = json.loads(response.content)
    # Check is sample missing, if so it's dirty by default
    if response_dict['messages']:
        if response_dict['messages'][0] == "'central_sample_id' key missing or empty" or not response_dict['success']:
            return True
    response_list = response_dict.get('get')
    if response_list:
        # Check biosample source id. 
        cog_biosample_source_id = list(response_list.values())[0]['biosample_sources'][0]['biosample_source_id']
        if sample['biosample_source_id'] != cog_biosample_source_id:
            return True
        sample.pop('biosample_source_id')
        # Check if keys in sample not in cog
        cog_record = Cogmeta(unknown = EXCLUDE).load(list(response_list.values())[0])
        if len(sample.keys() - cog_record.keys()) > 0:
            return True
        # Check if there are different values
        for k, v in sample.items():
            if cog_record[k] != v:
                return True
    return False
    

def majora_add_samples(sample_list, username, key, SERVER, dry = True):
    address = SERVER + '/api/v2/artifact/biosample/add/'
    payload = dict(username=username, token=key, client_name='cogsub', client_version='0.1')
    payload["biosamples"] = sample_list
    if not dry:
        response = requests.post(address, headers = {"Content-Type": "application/json", "charset": "UTF-8"}, json = payload)
        response_dict = json.loads(response.content)
        if response_dict['errors'] == 0:
            return True
        else:
            return False
    else:
        logging.debug(pprint.pprint(payload))
        return True

def majora_add_run(run_list, username, key, SERVER, dry = True):
    address = SERVER + '/api/v2/process/sequencing/add/'
    payload = dict(username=username, token=key, client_name='cogsub', client_version='0.1')
    payload.update(run_list)
    if not dry: 
        response = requests.post(address, headers = {"Content-Type": "application/json", "charset": "UTF-8"}, json = payload)
        response_dict = json.loads(response.content)
        if response_dict['errors'] == 0:
            return True
        else:
            return False
    else:
        logging.debug(pprint.pprint(payload))
        return True

def majora_add_library(library_list, username, key, SERVER, dry = True):
    address = SERVER + '/api/v2/artifact/library/add/'
    payload = dict(username=username, token=key, client_name='cogsub', client_version='0.1')
    payload.update(library_list)
    if not dry:
        response = requests.post(address, headers = {"Content-Type": "application/json", "charset": "UTF-8"}, json = payload)
        response_dict = json.loads(response.content)
        if response_dict['errors'] == 0:
            return True
        else:
            for x in response_dict['ignored']:
              #  if not majora_sample_exists(x , majora_username, majora_token, majora_server):
                pass
                   # print(x + ' Does not exists')
                
            return False
    else:
        logging.debug(pprint.pprint(payload))

  