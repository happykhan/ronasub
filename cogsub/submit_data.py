
import os 
import logging
import sys
import json 
import logging
import pprint
from marshmallow import EXCLUDE
from cogschemas import Cogmeta, CtMeta, RunMeta, LibraryBiosampleMeta, LibraryHeaderMeta
from climbfiles import ClimbFiles
from majora_util import majora_request
from collections import Counter
import re 
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from cogsub_util import load_config, most_frequent
import csv 

def read_illumina_dirs(output_dir_bams, output_dir_consensus, uploadlist, blacklist, climb_server_conn, climb_run_directory):
    found_samples = []
    for x in os.listdir(output_dir_bams):
        if x.startswith('NORW') and x.endswith('sorted.bam'):
            sample_name = x.split('_')[0]
            if sample_name.endswith('crude-prep'):
                continue
            if uploadlist: 
                if not sample_name in uploadlist:
                    logging.info('Skipping ' + sample_name)
                    continue
            if blacklist:
                if sample_name in blacklist:
                    logging.info('Skipping ' + sample_name)
                    continue
            bam_file = os.path.join(output_dir_bams, x)
            
            # Locate fasta file 
            fasta_file = [x for x in os.listdir(output_dir_consensus) if x.startswith(sample_name)]
            if len(fasta_file) == 1:
                fa_file_path = os.path.join(output_dir_consensus, fasta_file[0])
                # TODO make sure the sample name is valid 
                climb_sample_directory = os.path.join(climb_run_directory, sample_name)
                climb_server_conn.create_climb_dir(climb_sample_directory)
                climb_server_conn.put_file(fa_file_path, climb_sample_directory)
                climb_server_conn.put_file(bam_file, climb_sample_directory)
                found_samples.append(sample_name)
            elif len(fasta_file) == 0:
                logging.error('No fasta file!')
            else:
                logging.error('Multiple fasta file!')
    return found_samples

def read_ont_dirs(output_dir_bams, output_dir_consensus, uploadlist, blacklist, climb_server_conn, climb_run_directory):
    found_samples = []
    for x in os.listdir(output_dir_bams):
        sample_dir_match = re.match('(NORW-\w{5,6})', x)
#        valid_bam_match = re.match('(NORW-\w{5,6}).sorted.bam', x)
        if sample_dir_match: 
            sample_name = sample_dir_match.group(1)
            if uploadlist: 
                if not sample_name in uploadlist:
                    continue
            if blacklist:
                if sample_name in blacklist:
                    logging.info('Skipping ' + sample_name)
                    continue
            bam_file = os.path.join(output_dir_bams, sample_name, sample_name + '.sorted.bam')
            
            # Locate fasta file 
            fasta_file = [os.path.join(output_dir_consensus, sample_name, sample_name + '.consensus.fasta') for x in os.listdir(output_dir_consensus) if x == f'{sample_name}' ]
            if len(fasta_file) == 1:
                if os.path.exists(fasta_file[0]):
                    fa_file_path = os.path.join(output_dir_consensus, fasta_file[0])
                    # TODO make sure the sample name is valid 
                    climb_sample_directory = os.path.join(climb_run_directory, sample_name)
                    climb_server_conn.create_climb_dir(climb_sample_directory)
                    climb_server_conn.put_file(fa_file_path, climb_sample_directory)
                    climb_server_conn.put_file(bam_file, climb_sample_directory)
                    found_samples.append(sample_name)
                else:
                    logging.error('No fasta file for ' + sample_name)
            elif len(fasta_file) == 0:
                logging.error('No fasta file for ' + sample_name)
            else:
                logging.error('Multiple fasta file!')
    return found_samples    

def get_google_metadata(valid_samples, run_name, library_name, sheet_name, credentials='credentials.json', ont=False):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials, scope)
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
    force = True
    library_names = []
    in_run_but_not_in_sheet = list(set(valid_samples) - set(row_position))
    if in_run_but_not_in_sheet:
        logging.error('missing records in metadata sheet\n' + '\n'.join(in_run_but_not_in_sheet))
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
            if len(up_record.get('patient_group', '')) > 4: 
                #if up_record['patient_group'] != up_record['central_sample_id']:
                up_record['biosample_source_id'] = up_record['patient_group']
            up_record['metrics']['ct']['records']['1'] = dict(ct_value=ct_values.get('ct_1_ct_value', 0), test_kit=ct_values.get('ct_1_test_kit'), test_platform=ct_values.get('ct_1_test_platform'), test_target=ct_values.get('ct_1_test_target'))
            up_record['metrics']['ct']['records']['2'] = dict(ct_value=ct_values.get('ct_2_ct_value', 0), test_kit=ct_values.get('ct_2_test_kit'), test_platform=ct_values.get('ct_2_test_platform'), test_target=ct_values.get('ct_2_test_target'))
            if len(x.get('epi_cluster', '')) > 3:
                up_record['metadata'] = dict(epi=dict(cluster=x.get('epi_cluster')))

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
    return dict(biosamples= records_to_upload), library_to_upload

def cogsub_submit(majora_token, datadir, runname, sheet_name, gcredentials, ont, dry=False):
    # Load from config
    config = load_config(majora_token)
    output_dir = datadir
    run_name = runname
    library_name = 'NORW-' + run_name.split('_')[0]
    majora_username = config['majora_username']
    climb_file_server = config['climb_file_server']
    climb_username = config['climb_username'] 
    sheet_name = sheet_name
    logging.info(f'Dry run is {dry}')
    output_dir_bams = os.path.join(output_dir, 'ncovIllumina_sequenceAnalysis_trimPrimerSequences')
    output_dir_consensus = os.path.join(output_dir, 'ncovIllumina_sequenceAnalysis_makeConsensus')
    if ont:
        output_dir_bams = os.path.join(output_dir, "articNcovNanopore_sequenceAnalysisMedaka_articMinIONMedaka")
        output_dir_consensus = os.path.join(output_dir, "articNcovNanopore_sequenceAnalysisMedaka_articMinIONMedaka")
    climb_server_conn = ClimbFiles(climb_file_server, climb_username)
    # Does the run dir exist? 
    climb_server_conn.create_climb_dir('upload')
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
        blacklist = [x.strip() for x in open(output_dir_blacklist).readlines()]
    if ont:
        found_samples = read_ont_dirs(output_dir_bams, output_dir_consensus, uploadlist, blacklist, climb_server_conn, climb_run_directory)
    else:
        found_samples = read_illumina_dirs(output_dir_bams, output_dir_consensus, uploadlist, blacklist, climb_server_conn, climb_run_directory)
    # Connect to google sheet. Fetch & validate metadata
    logging.info(f'Found {len(found_samples)} samples')
    records_to_upload, library_to_upload = get_google_metadata(found_samples, run_name, library_name, sheet_name=sheet_name, credentials=gcredentials, ont=ont)
    # Connect to majora cog and sync metadata. 
    logging.info(f'Submitting biosamples to majora ' + run_name)
    if majora_request(records_to_upload, majora_username, config, 'api.artifact.biosample.add', dry):
        logging.info(f'Submitted biosamples to majora')
        logging.info(f'Submitting library and run to majora')
        for lib_val in library_to_upload.values():
            clean_lib_val = lib_val.copy()
            if len(clean_lib_val['biosamples']) > 0 : 
                run_to_upload = dict(library_name=lib_val['library_name'], runs = clean_lib_val.pop('runs'))
                
                majora_request(clean_lib_val, majora_username, config, 'api.artifact.library.add',  dry=dry)
                majora_request(run_to_upload, majora_username, config, 'api.process.sequencing.add', dry=dry)
    else:
        logging.error('failed to submit samples')