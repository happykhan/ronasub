from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import json 
from marshmallow import Schema, fields, validate, EXCLUDE, pre_load
import paramiko
import logging
import pprint

class Cogmeta(Schema):

    central_sample_id = fields.Str()
    adm1 = fields.Str()
    collection_date = fields.Date()
    received_date = fields.Date()
    source_age = fields.Integer(validate=validate.Range(min=0, max=110))
    source_sex = fields.Str(validate=validate.OneOf(["M", "F"]))
    adm2 = fields.Str()
    adm2_private = fields.Str()
    collecting_org = fields.Str()
    biosample_source_id = fields.Str()
    sample_type_collected = fields.Str()
    swab_site = fields.Str()    
    is_surveillance = fields.Str()
    is_icu_patient = fields.Str(validate=validate.OneOf(["Y", "N"]))

    @pre_load
    def clean_up(self, in_data, **kwargs):
        for k,v in dict(in_data).items():
            if v in ['', 'to check',  '#VALUE!', '-'] :
                in_data.pop(k)        
            elif isinstance(v, str):
                    in_data[k] = v.strip().upper()
            if in_data.get('is_icu_patient') not in ['Y', 'N']:
                if in_data.get('is_icu_patient'):
                    in_data.pop('is_icu_patient')        
        return in_data

class CtMeta(Schema):
    ct_1_ct_value = fields.Float(validate=validate.Range(min=0, max=60))
    ct_1_test_kit = fields.Str(validate=validate.OneOf(["ALTONA", "ABBOTT", "INHOUSE", "ROCHE", "AUSDIAGNOSTICS", "BOSPHORE", "SEEGENE"]))
    ct_1_test_platform = fields.Str(validate=validate.OneOf(["ALTOSTAR_AM16", "ABBOTT_M2000", "ROCHE_FLOW", "ROCHE_COBAS", "ELITE_INGENIUS", "CEPHEID_XPERT", "QIASTAT_DX", "AUSDIAGNOSTICS", "ROCHE_LIGHTCYCLER", "INHOUSE" ,"ALTONA", "PANTHER", "SEEGENE_NIMBUS"]))
    ct_1_test_target = fields.Str(validate=validate.OneOf(["S", "E", "RDRP", "N", "ORF1AB", "ORF8", "RDRP+N"]))    
    ct_2_ct_value = fields.Float(validate=validate.Range(min=0, max=60))
    ct_2_test_kit = fields.Str(validate=validate.OneOf(["ALTONA", "ABBOTT", "INHOUSE", "ROCHE", "AUSDIAGNOSTICS", "BOSPHORE", "SEEGENE"]))
    ct_2_test_platform = fields.Str(validate=validate.OneOf(["ALTOSTAR_AM16", "ABBOTT_M2000", "ROCHE_FLOW", "ROCHE_COBAS", "ELITE_INGENIUS", "CEPHEID_XPERT", "QIASTAT_DX", "AUSDIAGNOSTICS", "ROCHE_LIGHTCYCLER", "QIAGEN_ROTORGENE", "INHOUSE" ,"ALTONA", "PANTHER", "SEEGENE_NIMBUS"]))
    ct_2_test_target = fields.Str(validate=validate.OneOf(["S", "E", "RDRP", "N", "ORF1AB", "ORF8", "RDRP+N"]))    


    @pre_load
    def clean_up(self, in_data, **kwargs):
        for k,v in dict(in_data).items():
            if v in ['', 'to check',  '#VALUE!', '-'] :
                in_data.pop(k)        
            elif isinstance(v, str):
                    in_data[k] = v.strip().upper()
        return in_data

class RunMeta(Schema):
    run_name = fields.Str()
    instrument_make = fields.Str()
    instrument_model = fields.Str()

class LibraryHeaderMeta(Schema):
    library_name = fields.Str()
    library_seq_kit = fields.Str()
    library_seq_protocol = fields.Str()
    library_layout_config = fields.Str()

class LibraryBiosampleMeta(Schema):
    central_sample_id = fields.Str()
    library_selection = fields.Str()
    library_source = fields.Str()
    library_strategy = fields.Str()
    library_protocol = fields.Str(default='ARTIC')
    library_primers = fields.Integer()

def majora_sample_exists(sample_name, username, key, SERVER, dry = False):
    address = SERVER + '/api/v2/artifact/biosample/get/'
    payload = dict(central_sample_id=sample_name, username=username, token=key, client_name='cogsub', client_version='0.1')
    response = requests.post(address, headers = {"Content-Type": "application/json", "charset": "UTF-8"}, json = payload)
    if not dry:
        response_dict = json.loads(response.content)
        if response_dict['errors'] == 0:
            return True
        else:
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
                if not majora_sample_exists(x , majora_username, majora_token, majora_server):
                    print(x + ' Does not exists')
                
            return False
    else:
        logging.debug(pprint.pprint(payload))

from collections import Counter 
  
def most_frequent(List): 
    occurence_count = Counter(List) 
    return occurence_count.most_common(1)[0][0] 

def get_google_metadata(valid_samples, sheet_name):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('cogsub/credentials.json', scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name).sheet1
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)    
    all_values = sheet.get_all_records()
    records_to_upload = []
    run_to_upload = {} 
    library_to_upload = {}
    maybe_blacklist = []
    cells_to_update = []
    blank_cells_to_update = []
    force = False
    library_names = []
    in_run_but_not_in_sheet = list(set(valid_samples) - set([x['central_sample_id'] for x in all_values]))
    if in_run_but_not_in_sheet:
        logging.error('missing records in metadata sheet' + '\n'.join(in_run_but_not_in_sheet))
    for x in all_values:
       
        if x.get('central_sample_id', 'burnburnburn') in valid_samples:
            for k,v in dict(x).items():
                if v == '':
                    x.pop(k)
            # Fetch required fieldnames
            # Check if run_name is consistent & library name is consistent 
            library_names.append(library_name)
            if not x.get('run_name'):
                x['run_name'] = run_name
                blank_cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('run_name')+1, value=run_name))

            if x['run_name'] != run_name:
                logging.error('RUN NAME NOT CORRECT FOR ' + x['central_sample_id'] + ' Should be ' + run_name)
                maybe_blacklist.append(x['central_sample_id'])
                cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('run_name')+1, value=run_name))

            if not x.get('library_name'):
                x['library_name'] = library_name
                blank_cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('library_name')+1, value=library_name))                
            if x['library_name'] != library_name:
                logging.error('Library NAME NOT CORRECT FOR ' + x['central_sample_id'] + ' Should be ' + library_name)
                maybe_blacklist.append(x['central_sample_id'])
                cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('library_name')+1, value=library_name))                
            record = Cogmeta(unknown = EXCLUDE).load(x)
            # Get CT info
            up_record = Cogmeta().dump(record)
            ct_values = CtMeta(unknown=EXCLUDE).load(x)
            up_record['metrics'] = dict(ct=dict(records={}))

            up_record['metrics']['ct']['records']['1'] = dict(ct_value=ct_values.get('ct_1_ct_value', 0), test_kit=ct_values.get('ct_1_test_kit'), test_platform=ct_values.get('ct_1_test_platform'), test_target=ct_values.get('ct_1_test_target'))
            up_record['metrics']['ct']['records']['2'] = dict(ct_value=ct_values.get('ct_2_ct_value', 0), test_kit=ct_values.get('ct_2_test_kit'), test_platform=ct_values.get('ct_2_test_platform'), test_target=ct_values.get('ct_2_test_target'))
            records_to_upload.append(up_record)
            run_data = RunMeta(unknown = EXCLUDE).dump(x)
            library_sample_data = LibraryBiosampleMeta(unknown = EXCLUDE).dump(x)
            library_data = LibraryHeaderMeta(unknown = EXCLUDE).dump(x)
            # Handle libraries 
            if library_to_upload.get(library_data['library_name']):
                library_to_upload[library_data['library_name']]['biosamples'].append(library_sample_data)
                run_exists = False
                for existing_run in library_to_upload[library_data['library_name']]['runs']:
                    if run_data['run_name'] == existing_run['run_name']:
                        run_exists = True
                if not run_exists:
                    library_to_upload[library_data['library_name']]['runs'].append(run_data)
            else:
                library_to_upload[library_data['library_name']] = library_data
                library_to_upload[library_data['library_name']]['biosamples'] = [library_sample_data]
                library_to_upload[library_data['library_name']]['runs'] = [run_data]

    logging.warning('You may wish to blacklist:\n' + '\n'.join(maybe_blacklist))            
    if len(run_to_upload) > 1:
        print('Multiple runs in this directory')
    run_to_upload = dict(library_name = most_frequent(library_names), runs = list(run_to_upload.values()))
    if force:
        if cells_to_update:
            print('Updating values')
            sheet.update_cells(cells_to_update)
    if blank_cells_to_update:
            print('Updating values')
            sheet.update_cells(blank_cells_to_update)        
    return records_to_upload, library_to_upload

class ClimbFiles():

    def __init__(self, climb_file_server, climb_username):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(climb_file_server, username=climb_username)
        self.sftp = ssh.open_sftp() 

    def create_climb_dir(self, dir_name):
        try:
            self.sftp.stat(dir_name)
        except FileNotFoundError:
            logging.debug(f'dir {dir_name} on remote not found, creating')
            self.sftp.mkdir(dir_name)
        return dir_name

    def put_file(self, filename, path):
        remote_file_path = os.path.join(path, os.path.basename(filename))
        try:
            self.sftp.stat(remote_file_path)
        except FileNotFoundError:
            logging.debug(f'sending file {filename}')
            self.sftp.put(filename, remote_file_path) 
        return remote_file_path


def main(output_dir, run_name, library_name, majora_server, majora_username, majora_token, climb_file_server, climb_username, sheet_name, dry=True, force_sample_only=False):
    logging.debug(f'Dry run is {dry}')
    output_dir_bams = os.path.join(output_dir, 'ncovIllumina_sequenceAnalysis_readMapping')
    output_dir_consensus = os.path.join(output_dir, 'ncovIllumina_sequenceAnalysis_makeConsensus')
    found_samples = []
    climb_server_conn = ClimbFiles(climb_file_server, climb_username)
    # Does the run dir exist? 
    climb_run_directory = os.path.join('upload', run_name)
    climb_server_conn.create_climb_dir(climb_run_directory)
    # OPTIONAL. fetch upload list - in case only a subsample of results should be uploaded. 
    output_dir_uploadlist = os.path.join(output_dir, 'uploadlist')
    output_dir_blacklist = os.path.join(output_dir, 'blacklist')
    uploadlist = None
    blacklist = None
    if os.path.exists(output_dir_uploadlist):
        uploadlist = [x.strip() for x in open(output_dir_uploadlist).readlines()]
    if os.path.exists(output_dir_blacklist):
        blacklist = [x.strip() for x in open(output_dir_uploadlist).readlines()]
    for x in os.listdir(output_dir_bams):
        if x.startswith('E') and x.endswith('sorted.bam'):
            sample_name = x.split('_')[0]
            if uploadlist: 
                if not sample_name in uploadlist:
                    continue
            if blacklist:
                if sample_name in blacklist:
                    continue
            sample_name = x.split('_')[0]
            #sample_folder = os.path.join(run_path , 'NORW-' + sample_name)
            #if os.path.exists(sample_folder):
            #    os.mkdir(sample_folder)
            sample_name = x.split('_')[0]
            bam_file = os.path.join(output_dir_bams, x)
            
            # Locate fasta file 
            fasta_file = [x for x in os.listdir(output_dir_consensus) if x.startswith(sample_name)]
            if len(fasta_file) == 1:
                fa_file_path = os.path.join(output_dir_consensus, fasta_file[0])
                # TODO make sure the sample name is valid 
                climb_sample_directory = os.path.join(climb_run_directory, 'NORW-' + sample_name)
                climb_server_conn.create_climb_dir(climb_sample_directory)
                climb_server_conn.put_file(fa_file_path, climb_sample_directory)
                climb_server_conn.put_file(bam_file, climb_sample_directory)
                found_samples.append('NORW-' + sample_name)
            elif len(fasta_file) == 0:
                logging.error('No fasta file!')
            else:
                logging.error('Multiple fasta file!')

    # Connect to google sheet. Fetch & validate metadata
    logging.debug(f'Found {len(found_samples)} samples')
    records_to_upload, library_to_upload = get_google_metadata(found_samples, sheet_name=sheet_name)

    # Connect to majora cog and sync metadata. 
    logging.debug(f'Submitting biosamples to majora ' + run_name)
    samples_dont_exist = [] 
    if force_sample_only:
        majora_add_samples(records_to_upload, majora_username, majora_token, majora_server, dry)
    else:
        for biosample in records_to_upload:
            if not majora_sample_exists(biosample['central_sample_id'], majora_username, majora_token, majora_server, dry=False):
                samples_dont_exist.append(biosample['central_sample_id'] )
        if majora_add_samples(records_to_upload, majora_username, majora_token, majora_server, dry):
            logging.debug(f'Submitted biosamples to majora')
#            if len(samples_dont_exist) > 0 :
            logging.debug(f'Submitting library and run to majora')
            for lib_val in library_to_upload.values():
                clean_lib_val = lib_val.copy()
                #clean_lib_val['biosamples'] = [x for x in clean_lib_val['biosamples'] if x['central_sample_id'] in samples_dont_exist]
                # You shouldn't touch libraries for existing samples i.e we only submit new runs. 
                if len(clean_lib_val['biosamples']) > 0 : 
                    run_to_upload = dict(library_name=lib_val['library_name'], runs = clean_lib_val.pop('runs'))
                    
                    majora_add_library(clean_lib_val, majora_username, majora_token, majora_server, dry=dry)
                    majora_add_run(run_to_upload, majora_username, majora_token, majora_server, dry=dry)
        else:
            logging.error('failed to submit samples')


# Look in analysis output dir and get list of samples with consensus and mapped reads 
logging.basicConfig(level=logging.DEBUG)

# global
# TEST SETTINGS 
majora_server = 'https://covid.majora.ironowl.it'
majora_username = 'test-climb-covid19-alikhann'
majora_token = 'def6325a-4c14-40b4-b515-f060c7c03158'

# REAL SETTINGS 
majora_server = 'https://majora.covid19.climb.ac.uk'
majora_username = 'climb-covid19-alikhann'
majora_token = '60b823ba-ca95-4919-b7c7-e9379c1fcd61'
climb_file_server = 'bham.covid19.climb.ac.uk'
climb_username = 'climb-covid19-alikhann'
sheet_name = 'SARCOV2-Metadata'


# run 1
output_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20200418/'
run_name = '200418_NB501819_0131_AH5TWFAFX2'
library_name = 'NORW-' + run_name.split('_')[0]

#main(output_dir, run_name, library_name, majora_server, majora_username, majora_token, climb_file_server, climb_username, sheet_name, dry=False, force_sample_only=True)
        
# # run 2
output_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20200429/'
run_name = '200429_NB501819_0132_AH5J7GAFX2'
library_name = 'NORW-' + run_name.split('_')[0]

#main(output_dir, run_name, library_name, majora_server, majora_username, majora_token, climb_file_server, climb_username, sheet_name, dry=False, force_sample_only=True)        

# # run 3
output_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20200513/'
run_name = '200513_NB501819_0135_AH5JCCAFX2'
library_name = 'NORW-' + run_name.split('_')[0]

#main(output_dir, run_name, library_name, majora_server, majora_username, majora_token, climb_file_server, climb_username, sheet_name, dry=False, force_sample_only=True)

# run 4
output_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20200519/'
run_name = '200519_NB501819_0137_AH5YM5AFX2'
library_name = 'NORW-' + run_name.split('_')[0]

#main(output_dir, run_name, library_name, majora_server, majora_username, majora_token, climb_file_server, climb_username, sheet_name, dry=False, force_sample_only=True)

# run 5
output_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20200522/'
run_name = '200522_NB501819_0138_AH722WAFX2'
library_name = 'NORW-' + run_name.split('_')[0]

#main(output_dir, run_name, library_name, majora_server, majora_username, majora_token, climb_file_server, climb_username, sheet_name, dry=False, force_sample_only=True)

# run 6
output_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20200602.filtered_cont_reads_all_min_cov_100/'
run_name = '200602_NB501819_0139_AH5W5VAFX2'
library_name = 'NORW-' + run_name.split('_')[0]

#main(output_dir, run_name, library_name, majora_server, majora_username, majora_token, climb_file_server, climb_username, sheet_name, dry=False, force_sample_only=True)

# run 8
output_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20200610/'
run_name = '200610_NB501819_0140_AH5Y2LAFX2'
library_name = 'NORW-' + run_name.split('_')[0]

#main(output_dir, run_name, library_name, majora_server, majora_username, majora_token, climb_file_server, climb_username, sheet_name, dry=False, force_sample_only=True)

output_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20200617'
run_name = '200617_NB501819_0142_AH5W3YAFX2'
library_name = 'NORW-' + run_name.split('_')[0]

#main(output_dir, run_name, library_name, majora_server, majora_username, majora_token, climb_file_server, climb_username, sheet_name, dry=False, force_sample_only=False)

output_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20200624'
run_name = '200624_NB501819_0143_AH5VV7AFX2'
library_name = 'NORW-' + run_name.split('_')[0]

main(output_dir, run_name, library_name, majora_server, majora_username, majora_token, climb_file_server, climb_username, sheet_name, dry=False, force_sample_only=False)
