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
from climbfiles import ClimbFiles
import os 
from submit_schema import  Samplemeta

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
                if row['central_sample_id'] != '':
                    new_row = [row['central_sample_id'], row['library_name'], row['run_name']] 
                    sample_list.append(new_row)
        if sample_list:
            submission_sheet.resize(len(submission_values))
            submission_sheet.append_rows(sample_list)

def check_meta(majora_token, sheet_name, submission_sheet_name, gcredentials):
    # Update missing rows 
    add_missing_rows(submission_sheet_name, gcredentials)

    # Fetch metadata from remote
    config = load_config(majora_token)    
    climb_file_server = config['climb_file_server']
    climb_username = config['climb_username'] 
    climb_server_conn = ClimbFiles(climb_file_server, climb_username)
    cog_metadata = climb_server_conn.get_metadata('temp/')
    cog_matched_metadata = climb_server_conn.get_metadata('temp/', matched=True)

    config = load_config(majora_token)
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(gcredentials, scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name).sheet1

    all_values = sheet.get_all_records()
    cells_to_update = []

    submission_sheet = client.open(submission_sheet_name).sheet1
    column_position = submission_sheet.row_values(1)
    row_position = submission_sheet.col_values(1)
    logging.info(f'loaded {len(all_values)} records from submission sheet {sheet_name}')
    for x in all_values:
        sample_name = x.get('central_sample_id')
        if sample_name in row_position:
            cog_value = ""
            partial_value = ''
            if cog_metadata.get(sample_name):
                cog_value = "YES"
                # local = Samplemeta(unknown = EXCLUDE).load(x)                
                #   remote =  Samplemeta(unknown = EXCLUDE).load(cog_metadata[sample_name])       
                # shared_items = {k: x[k] for k in x if k in cog_metadata[sample_name] and x[k] == cog_metadata[sample_name][k]}
                # TODO: Should check record values of COG versus local copy. 
                metadata_sync_errors = ''
                if cog_metadata[sample_name]['run_name'] != x['run_name']:
                    remote_run_name = cog_metadata[sample_name]['run_name']
                    metadata_sync_errors += f'run name is {remote_run_name} on COG, '
                if cog_metadata[sample_name]['biosample_source_id'] != x['biosample_source_id']:
                    biosample_source_id = cog_metadata[sample_name]['biosample_source_id']
                    metadata_sync_errors += f'run name is {biosample_source_id} on COG, '
                if cog_metadata[sample_name]['library_name'] != x['library_name']:
                    library_name = cog_metadata[sample_name]['library_name']
                    metadata_sync_errors += f'run name is {library_name} on COG, '                        
                cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('upload_date')+1, value=cog_metadata[sample_name]['sequencing_submission_date']))
                cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('pags')+1, value=cog_metadata[sample_name]['published_as']))
                cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('pag_count')+1, value=len(cog_metadata[sample_name]['published_as'].split(','))))
                cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('metadata_sync')+1, value=metadata_sync_errors))
                if sample_name not in cog_matched_metadata.keys():
                    partial_value = 'YES'
            cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('is_submitted_to_cog')+1, value=cog_value))
            cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('partial_submission')+1, value=partial_value))
        else:
            logging.error(f'{sample_name} not found in submission sheet ')
    if cells_to_update:
        print('Updating values')
        submission_sheet.update_cells(cells_to_update)          


def load_config(config="majora.json"):
    config_dict = json.load(open(config))
    return config_dict