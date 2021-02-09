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
from reactschemas import Samplemeta, RunMeta, LibraryBiosampleMeta, LibraryHeaderMeta
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

def get_metadata(valid_samples, run_name, all_values, library_name, ont=False):
    records_to_upload = []
    run_to_upload = {} 
    library_to_upload = {}
    library_names = []
    for this_sample in valid_samples:
        this_record = [x for x in all_values.values() if x['cleaned_sample_name'] == this_sample][0]
        this_record['library_name'] = library_name
        this_record['run_name'] = run_name
        # Fetch required fieldnames
        # Check if run_name is consistent & library name is consistent 
        library_names.append(library_name)
        record = Samplemeta(unknown = EXCLUDE).load(this_record)
        # Get CT info
        up_record = Samplemeta().dump(record)
        records_to_upload.append(up_record)
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
    run_to_upload = dict(library_name = most_frequent(library_names), runs = list(run_to_upload.values()))
    return dict(biosamples= records_to_upload), library_to_upload

def load_config(config="majora.json"):
    config_dict = json.load(open(config))
    return config_dict

def read_illumina_dirs(output_dir_bams, output_dir_consensus, uploadlist, blacklist, climb_server_conn, climb_run_directory):
    found_samples = []
    for x in os.listdir(output_dir_bams):
        if x.startswith('ARCH') and x.endswith('sorted.bam'):
            sample_name = x.split('_')[0]
            if uploadlist: 
                if not x.split('.')[0] in uploadlist:
                    logging.info('Skipping ' + sample_name)
                    continue
            if blacklist:
                if x in blacklist:
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

import csv 
def get_sample_info(sheet_name, credentials='credentials.json'): 
    # Special samples to look for : 
    white_list = {} 
    for x in csv.DictReader(open(sheet_name), dialect=csv.excel_tab):
        if x.get('pangolin_lineage') == 'B.1.351':
            white_list[x['sample_name']] = x 
    return white_list


def get_dirs(valid_samples, datadir):  
    seq_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Nextseq_2_runs'
    data_to_upload = [] 
    for react_dir in [os.path.join(datadir, x) for x in os.listdir(datadir) if x.endswith('REACT') and x.startswith('result')]:
        qc_file = [os.path.join(react_dir, x) for x in os.listdir(react_dir) if x.endswith('-REACT.qc.csv')]
        if len(qc_file) == 1:
            sample_names  = [x['sample_name'] for x in csv.DictReader(open(qc_file[0])) ]
            valid_sample_names = [x['sample_name'] for x in valid_samples.values() ]
            overlap = set(valid_sample_names) & set(sample_names)
            if len(overlap) > 0 : 
                logging.info(f'Valid samples in {react_dir}')
                library_name = os.path.basename(react_dir).replace('result.illumina.', '')
                run_name = [x for x in os.listdir(seq_dir) if x.startswith(library_name[2:8])]
                if len(run_name) == 1 :
                    data_to_upload.append({"output_dir": react_dir, "library_name": 'NORW-' + library_name, "run_name": run_name[0] + '-REACT'})   
                else:
                    logging.info(f'Multiple possible run names  {run_name}')
                with open(react_dir + '/uploadlist', 'w') as j:
                    j.write('\n'.join(list(overlap)))
    return data_to_upload
        # library_name = 'ARCH-' + run_name.split('_')[0]

def reactsub_run(majora_token, datadir, sheet_name, ont, dry=False):
    # Load from config
    config = load_config(majora_token)
    majora_username = config['majora_username']
    climb_file_server = config['climb_file_server']
    climb_username = config['climb_username']     
    logging.info(f'Dry run is {dry}')
    valid_samples = get_sample_info(args.sheet_name) 
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
            found_samples = read_illumina_dirs(output_dir_bams, output_dir_consensus, uploadlist, blacklist, climb_server_conn, climb_run_directory)
        # Connect to google sheet. Fetch & validate metadata
        logging.info(f'Found {len(found_samples)} samples')
        records_to_upload, library_to_upload = get_metadata(found_samples, run_name, valid_samples, library_name, ont=ont)
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
    parser.add_argument('--sheet_name', action='store', default='react_file', help='Master Table file ')
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
