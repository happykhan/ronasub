"""
reactsub submits react metadata and sequences to COG server 

Requires login for google sheets

### CHANGE LOG ### 
2021-02-09 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build
"""
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

from oauth2client.service_account import ServiceAccountCredentials
import gspread

epi = "Licence: " + meta.__licence__ +  " by " +meta.__author__ + " <" +meta.__author_email__ + ">"
logging.basicConfig()
log = logging.getLogger()


def most_frequent(List): 
    occurence_count = Counter(List) 
    return occurence_count.most_common(1)[0][0] 

def get_metadata(valid_samples, run_name, sheet_name, library_name, ont=False, credentials='credentials.json'):
    records_to_upload = []
    run_to_upload = {} 
    library_to_upload = {}
    library_names = []
    cells_to_update = []
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials, scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name).sheet1
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)    
    all_values = sheet.get_all_records()
    for this_record in all_values:
        if this_record.get('manual_qc_pass') != 'FALSE' and this_record.get('sample_barcode') and this_record['original_sample_id'] in valid_samples and (this_record['received_date'] or this_record['collection_date']):
            # backfill library if unknown 
            if not this_record.get('central_sample_id'):
                if this_record['original_sample_id'].startswith('ARCH'):
                    this_record['central_sample_id'] =  this_record['cleaned_sample_name']
                else:
                    this_record['central_sample_id'] = 'ARCH-' + this_record['seq_id']
                cells_to_update.append(gspread.models.Cell(row=row_position.index(this_record['original_sample_id'])+1, col=column_position.index('central_sample_id')+1, value=this_record['central_sample_id']))
            if not this_record.get('biosample_source_id'):
                this_record['biosample_source_id'] = this_record['sample_barcode']
                cells_to_update.append(gspread.models.Cell(row=row_position.index(this_record['original_sample_id'])+1, col=column_position.index('biosample_source_id')+1, value=this_record['biosample_source_id']))                            
            if not this_record.get('library_name'):
                cells_to_update.append(gspread.models.Cell(row=row_position.index(this_record['original_sample_id'])+1, col=column_position.index('library_name')+1, value=library_name))            
                this_record['library_name'] = library_name
            if not this_record.get('run_name'):
                cells_to_update.append(gspread.models.Cell(row=row_position.index(this_record['original_sample_id'])+1, col=column_position.index('run_name')+1, value=run_name))            
                this_record['run_name'] = run_name
            if not this_record.get('is_submitted_to_cog'):
                cells_to_update.append(gspread.models.Cell(row=row_position.index(this_record['original_sample_id'])+1, col=column_position.index('is_submitted_to_cog')+1, value='YES'))            
            if not this_record.get('collecting_org'):
                cells_to_update.append(gspread.models.Cell(row=row_position.index(this_record['original_sample_id'])+1, col=column_position.index('collecting_org')+1, value='REACT'))                            
                this_record['collecting_org'] = 'REACT'
            # Fetch required fieldnames
            # Check if run_name is consistent & library name is consistent 
            library_names.append(library_name)
            # Get CT info
            record = Cogmeta(unknown = EXCLUDE).load(this_record)

            up_record = Cogmeta().dump(record)
            up_record.pop('collection_pillar', None)
            records_to_upload.append(up_record)
            if not this_record.get('adm2'):
                cells_to_update.append(gspread.models.Cell(row=row_position.index(this_record['original_sample_id'])+1, col=column_position.index('adm2')+1, value=up_record.get('adm2')))
            if not this_record.get('adm1'):
                cells_to_update.append(gspread.models.Cell(row=row_position.index(this_record['original_sample_id'])+1, col=column_position.index('adm1')+1, value=up_record.get('adm1')))
            run_data = RunMeta(unknown = EXCLUDE).dump(this_record)
            library_sample_data = LibraryBiosampleMeta(unknown = EXCLUDE).dump(this_record)
            library_data = LibraryHeaderMeta(unknown = EXCLUDE).dump(this_record)

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

    if len(run_to_upload) > 1:
        print('Multiple runs in this directory')
    if cells_to_update:
        print('Updating values')
        sheet.update_cells(cells_to_update)        
    run_to_upload = dict(library_name = most_frequent(library_names), runs = list(run_to_upload.values()))
    return dict(biosamples= records_to_upload), library_to_upload

def load_config(config="majora.json"):
    config_dict = json.load(open(config))
    return config_dict

def read_illumina_dirs(output_dir_bams, output_dir_consensus, uploadlist, blacklist, climb_server_conn, climb_run_directory, sample_map = None, existing=None):
    found_samples = []
    for bam_file in os.listdir(output_dir_bams):
        if bam_file.endswith('sorted.bam'):
            if existing:
                if not bam_file.split('.')[0] in existing:
                    logging.info('Skipping EXISTING ' + bam_file)
                    continue
            if uploadlist: 
                if not bam_file.split('.')[0] in uploadlist:
                    logging.info('Skipping ' + bam_file)
                    continue
            if blacklist:
                if bam_file.split('.')[0] in blacklist:
                    logging.info('Skipping ' + bam_file)
                    continue
            bam_file_path = os.path.join(output_dir_bams, bam_file)
            
            # Locate fasta file 
            fasta_file = [x for x in os.listdir(output_dir_consensus) if x.split('.')[0] == bam_file.split('.')[0]]
            if len(fasta_file) == 1:
                fa_file_path = os.path.join(output_dir_consensus, fasta_file[0])
                if sample_map.get(bam_file.split('.')[0]):
                    upload_sample_name = sample_map.get(bam_file.split('.')[0])
                else:
                    upload_sample_name = bam_file.split('.')[0].split('_')[0]
                # TODO make sure the sample name is valid 
                climb_sample_directory = os.path.join(climb_run_directory, upload_sample_name)
                climb_server_conn.create_climb_dir(climb_sample_directory)
                climb_server_conn.put_file(fa_file_path, climb_sample_directory)
                climb_server_conn.put_file(bam_file_path, climb_sample_directory)
                found_samples.append(bam_file.split('.')[0])
            elif len(fasta_file) == 0:
                logging.error('No fasta file!')
            else:
                logging.error('Multiple fasta file!')
    return found_samples

def read_ont_dirs(output_dir_bams, output_dir_consensus, uploadlist, blacklist, climb_server_conn, climb_run_directory):
    found_samples = []
    for x in os.listdir(output_dir_bams):
        valid_bam_match = re.match('(ARCH-\w{5,6}).sorted.bam', x)

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

def get_dirs(valid_samples, datadir):  
    seq_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Nextseq_2_runs'
    data_to_upload = [] 
    for react_dir in sorted([os.path.join(datadir, x) for x in os.listdir(datadir) if x.endswith('REACT') and x.startswith('result')], reverse=True):
        qc_file = [os.path.join(react_dir, x) for x in os.listdir(react_dir) if x.endswith('-REACT.qc.csv')]
        if len(qc_file) == 1:
            sample_names  = [x['sample_name'] for x in csv.DictReader(open(qc_file[0])) ]
            valid_sample_names = [x['original_sample_id'] for x in valid_samples.values() ]
            overlap = set(valid_sample_names) & set(sample_names)
            logging.info(f'Looking at dir in {react_dir}')
            if len(overlap) > 0 : 
                logging.info(f'Valid samples in {react_dir}')             
                library_name = os.path.basename(react_dir).replace('result.illumina.', '')
                run_name = [x for x in os.listdir(seq_dir) if x.startswith(library_name[2:8])]
                if len(run_name) == 1 :
                    
                    plate_info =  {}
                    # Open Sample sheet
                #     j = [x.encode("ascii", "ignore") for x in open(f'{seq_dir}/{run_name[0]}/SampleSheet2.csv' )]
                    sample_path_name = f'{seq_dir}/{run_name[0]}/SampleSheet2.csv' 
                    if os.path.exists(sample_path_name): 
                        lines = [x for x in open(sample_path_name) if x.strip().endswith('REACT') or x.startswith('Sample_name')] 
                    else:
                        lines = [x for x in open(f'{seq_dir}/{run_name[0]}/SampleSheet.csv' ) if x.strip().endswith('REACT') or x.startswith('Sample_name')] 
                    for x in csv.DictReader(lines):
                        if x['Project'].upper() == 'REACT':
                            if plate_info.get(x['Called']):
                                plate_info[x['Called']].append(x['Sample_name'])
                            else:
                                plate_info[x['Called']] = [x['Sample_name']]    
                    samples = [item for sublist in plate_info.values() for item in sublist]
                    sample_map = {x: 'ARCH-' + valid_samples[x]['seq_id']  for x in overlap if not x.startswith('ARCH')}
                    data_to_upload.append({"output_dir": react_dir, "library_name": 'NORW-' + library_name, "run_name": run_name[0] + '-REACT', "sample_map": sample_map})   
                else:
                    logging.info(f'Multiple possible run names  {run_name}')
                with open(react_dir + '/uploadlist', 'w') as j:
                    j.write('\n'.join(list(overlap)))
                
    return data_to_upload
        # library_name = 'ARCH-' + run_name.split('_')[0]

def get_valid_samples(sheet_name, credentials='credentials.json'):
    valid_samples = {} 
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials, scope)
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()

    for x in all_values:
        # if this_record.get('manual_qc_pass') != 'FALSE' and this_record.get('sample_barcode') and this_record['original_sample_id'] in valid_samples and (this_record['received_date'] or this_record['collection_date'])
#        if x.get('is_submitted_to_cog') != 'YES' and x.get('wave') in [9] and x.get('sample_barcode') != '' and x.get('manual_qc_pass') != 'FALSE' and (x['received_date'] or x['collection_date']) :
        if x.get('is_submitted_to_cog') != 'YES' and x.get('sample_barcode') != '' and x.get('manual_qc_pass') != 'FALSE' and (x['received_date'] or x['collection_date']) :

            valid_samples[x.get('original_sample_id')] = x 
    return valid_samples 

def reactsub_run(majora_token, datadir, sheet_name, ont, dry=False):
    config = load_config(majora_token)
    majora_username = config['majora_username']
    climb_file_server = config['climb_file_server']
    climb_username = config['climb_username']     
    logging.info(f'Dry run is {dry}')
    valid_samples = get_valid_samples(sheet_name)
    done = [ ]
    for values in get_dirs(valid_samples, datadir):
        output_dir = values['output_dir']
        library_name = values['library_name']
        run_name = values['run_name']
        # OPTIONAL. fetch upload list - in case only a subsample of results should be uploaded. 
        output_dir_uploadlist = os.path.join(output_dir, 'uploadlist')
        output_dir_blacklist = os.path.join(output_dir, 'blacklist')
        uploadlist = None
        blacklist = None
        if os.path.exists(output_dir_uploadlist):
            uploadlist = [x.strip() for x in open(output_dir_uploadlist).readlines()]
        if os.path.exists(output_dir_blacklist):
            blacklist = [x.strip() for x in open(output_dir_blacklist).readlines()]        
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
        if ont:
            found_samples = read_ont_dirs(output_dir_bams, output_dir_consensus, uploadlist, blacklist, climb_server_conn, climb_run_directory)
        else:
            found_samples = read_illumina_dirs(output_dir_bams, output_dir_consensus, uploadlist, blacklist, climb_server_conn, climb_run_directory, sample_map=values["sample_map"], existing=done)
        # Connect to google sheet. Fetch & validate metadata
        done += found_samples
        logging.info(f'Found {len(found_samples)} samples')
        records_to_upload, library_to_upload = get_metadata(found_samples, run_name, sheet_name, library_name, ont=ont)
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


def main(args):
    reactsub_run(args.majora_token, args.datadir, args.sheet_name, args.ont)
   
if __name__ == '__main__':
    start_time = time.time()
    log.setLevel(logging.INFO)
    desc = __doc__.split('\n\n')[0].strip()
    parser = argparse.ArgumentParser(description=desc,epilog=epi)
    parser.add_argument ('-v', '--verbose', action='store_true', default=False, help='verbose output')
    parser.add_argument('--version', action='version', version='%(prog)s ' + meta.__version__)
    parser.add_argument('datadir', action='store', help='Location of ARTIC pipeline output')
    parser.add_argument('--sheet_name', action='store', default='SARSCOV2-REACT-Metadata', help='Master Table file ')
    parser.add_argument('--majora_token', action='store', default='majora.json', help='Path to MAJORA COG API credentials (JSON)')
    parser.add_argument('--ont', action='store_true', default=False, help='Is the output directory from nanopore')
    args = parser.parse_args()
    if args.verbose: 
        log.setLevel(logging.DEBUG)
        log.debug( "Executing @ %s\n"  %time.asctime())    
    main(args)
    if args.verbose: 
        log.debug("Ended @ %s\n"  %time.asctime())
        log.debug('total time in minutes: %d\n' %((time.time() - start_time) / 60.0))
