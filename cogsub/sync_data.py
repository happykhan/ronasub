from oauth2client.service_account import ServiceAccountCredentials
import gspread
from cogsub_util import load_config, clean_dict, prepare_meta_record, chunks
from majora_util import majora_request
import logging 
from cogschemas import  RunMeta, LibraryBiosampleMeta, LibraryHeaderMeta
from marshmallow import EXCLUDE

def cogsub_sync(majora_token, sheet_name,  credentials='credentials.json', dry=False):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials, scope)
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    records_to_upload = []
    library_to_upload = {}
    for x in all_values:
        if x.get('run_name') and x.get('library_name') and x.get('received_date').startswith('2021'):
            up_record = prepare_meta_record(x)
            if up_record:
                records_to_upload.append(up_record)
            else:
                logging.error('This came back null' )


            run_data = RunMeta(unknown = EXCLUDE).load(x)
            library_sample_data = LibraryBiosampleMeta(unknown = EXCLUDE).load(x)
            library_data = LibraryHeaderMeta(unknown = EXCLUDE).load(x)
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


    config = load_config(majora_token)
    majora_username = config['majora_username']

    for lib_val in  library_to_upload.values():
        clean_lib_val = lib_val.copy()
        if len(clean_lib_val['biosamples']) > 0 : 
            run_to_upload = dict(library_name=lib_val['library_name'], runs = clean_lib_val.pop('runs'))
            
            majora_request(clean_lib_val, majora_username, config, 'api.artifact.library.add',  dry=dry)
           # majora_request(run_to_upload, majora_username, config, 'api.process.sequencing.add', dry=dry)        

    for record_chunk in chunks(records_to_upload, 200):
        majora_request(dict(biosamples=record_chunk), majora_username, config, 'api.artifact.biosample.add', dry)
    