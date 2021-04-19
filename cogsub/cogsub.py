"""
cogsub submits metadata and sequences to COG server 

Requires login for google sheets

### CHANGE LOG ### 
2020-08-17 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build - split from dirty scripts
"""
from __future__ import print_function
import pickle
import os.path
import time
import argparse
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import meta
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

epi = "Licence: " + meta.__licence__ +  " by " +meta.__author__ + " <" +meta.__author_email__ + ">"
logging.basicConfig()
log = logging.getLogger()


def most_frequent(List): 
    occurence_count = Counter(List) 
    return occurence_count.most_common(1)[0][0] 

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

def load_config(config="majora.json"):
    config_dict = json.load(open(config))
    return config_dict

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
        valid_bam_match = re.match('(NORW-\w{5,6}).sorted.bam', x)

        if valid_bam_match:
            sample_name = valid_bam_match.group(1)
            if uploadlist: 
                if not sample_name in uploadlist:
                    continue
            if blacklist:
                if sample_name in blacklist:
                    logging.info('Skipping ' + sample_name)
                    continue
            bam_file = os.path.join(output_dir_bams, x)
            
            # Locate fasta file 
            fasta_file = [x for x in os.listdir(output_dir_consensus) if x == f'{sample_name}.consensus.fasta' ]
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

def output_to_csv(records_to_upload):
    output_csv = open('metadata.csv','w+')
    header_has_been_printed=False
    first_key=True

    key_set=('central_sample_id','collecting_org','collection_date','biosample_source_id','is_surveillance','adm2','source_age','source_sex','is_icu_patient','ct_1_ct_value','ct_2_ct_value','swab_site','adm1')

    for patient_record in records_to_upload['biosamples']:
        if not header_has_been_printed: 
            for key in key_set:
                if not first_key: output_csv.write(',')
                first_key=False
                output_csv.write(key)
            output_csv.write('\n')
            header_has_been_printed=True

        first_key=True
        for key in key_set:
            if not first_key: output_csv.write(',')
            first_key=False
            if key in patient_record.keys():
                my_value = str(patient_record[key])
                if ',' in my_value: output_csv.write('"')
                output_csv.write(my_value)
                if ',' in my_value: output_csv.write('"')
            elif key[:3]=='ct_':
                my_metrics = patient_record['metrics']
                ct = my_metrics['ct']
                ct_records = ct['records']
                for index in ('1','2'):
                    if key[:4]=='ct_' + index:
                        ct_records_index = ct_records[index]
                        ct_records_index_value = ct_records_index['ct_value']
                        output_csv.write(str(ct_records_index_value))

        output_csv.write('\n') # Assume the sample is from surveillance

    output_csv.flush()
    output_csv.close()



def cogsub_run(majora_token, datadir, runname, sheet_name, gcredentials, force_sample_only, ont, dry=False):
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
    output_to_csv(records_to_upload)
    if force_sample_only:
        majora_request(records_to_upload, majora_username, config, 'api.artifact.biosample.add', dry)
    else:
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


def main(args):
    cogsub_run(args.majora_token, args.datadir, args.runname, args.sheet_name,  args.gcredentials, args.force_sample_only, args.ont)
   
if __name__ == '__main__':
    start_time = time.time()
    log.setLevel(logging.INFO)
    desc = __doc__.split('\n\n')[0].strip()
    parser = argparse.ArgumentParser(description=desc,epilog=epi)
    parser.add_argument ('-v', '--verbose', action='store_true', default=False, help='verbose output')
    parser.add_argument('--version', action='version', version='%(prog)s ' + meta.__version__)
    parser.add_argument('datadir', action='store', help='Location of ARTIC pipeline output')
    parser.add_argument('runname', action='store', help='Sequencing run name, must be unique')
    parser.add_argument('--gcredentials', action='store', default='credentials.json', help='Path to Google Sheets API credentials (JSON)')
    parser.add_argument('--sheet_name', action='store', default='SARCOV2-Metadata', help='Name of Master Table in Google sheets')
    parser.add_argument('--majora_token', action='store', default='majora.json', help='Path to MAJORA COG API credentials (JSON)')
    parser.add_argument('--force_sample_only', action='store_true', default=False, help='Just update sample metadata (do not submit library or sequencing info)')
    parser.add_argument('--ont', action='store_true', default=False, help='Is the output directory from nanopore')
    args = parser.parse_args()
    if args.verbose: 
        log.setLevel(logging.DEBUG)
        log.debug( "Executing @ %s\n"  %time.asctime())    
    main(args)
    if args.verbose: 
        log.debug("Ended @ %s\n"  %time.asctime())
        log.debug('total time in minutes: %d\n' %((time.time() - start_time) / 60.0))
