
import logging 
import os 
import re 
import csv 
from climbfiles import ClimbFiles
import json 

def load_config(config="majora.json"):
    config_dict = json.load(open(config))
    return config_dict

def get_file_paths(datadir):
    found_samples = [] 
    output_dir_uploadlist = os.path.join(datadir, 'uploadlist')
    output_dir_blacklist = os.path.join(datadir, 'blacklist')
    uploadlist = None
    blacklist = None
    if os.path.exists(output_dir_uploadlist):
        uploadlist = [x.strip() for x in open(output_dir_uploadlist).readlines()]
    if os.path.exists(output_dir_blacklist):
        blacklist = [x.strip() for x in open(output_dir_blacklist).readlines()]  
    qc_files = [os.path.join(datadir, x) for x in os.listdir(datadir) if x.endswith('.qc.csv')] 
    if len(qc_files) == 1:
        for row in csv.DictReader(open(qc_files[0])):
            sample_name = row['sample_name']
            # Handle the _S123 suffix on the sample name if any. 
            S_suffix = re.match('(.+)_S\d+', sample_name)
            if S_suffix:
                sample_name = S_suffix.group(1)
            if uploadlist: 
                if not sample_name in uploadlist:
                    logging.info('Skipping ' + sample_name)
                    continue                
            if blacklist:
                if sample_name in blacklist:
                    logging.info('Skipping ' + sample_name)
                    continue
            run_name = [x for x in os.listdir( os.path.join(datadir, 'qc_climb_upload') )][0]
            output_dir_consensus = os.path.join(datadir, 'qc_climb_upload', run_name, row['sample_name'], row['fasta'])
            output_dir_bam = os.path.join(datadir, 'qc_climb_upload', run_name, row['sample_name'], row['bam'])
            found_sample = dict(sample_name=sample_name, fasta=output_dir_consensus, bam=output_dir_bam)
            if os.path.exists(found_sample['bam']) and os.path.exists(found_sample['fasta']):
                found_samples.append(found_sample)
            else: 
                # Try alternate filename for bam. 
                found_sample['bam'] = os.path.join(datadir, 'qc_climb_upload', run_name, row['sample_name'], row['sample_name'] + '.mapped.bam') 
                if os.path.exists(found_sample['bam']) and os.path.exists(found_sample['fasta']):
                    found_samples.append(found_sample)                
                else:
                    logging.error(f'Could not locate bam and fasta for {sample_name} in {datadir}')
        return found_samples
    else:
        logging.error(f'Multiple QC files in dir {datadir}')
        return None 

def legacy_submit_filedata(datadir, run_name, majora_token):

    # If a datadir is given, just upload the files. (legacy method)

    found_samples = get_file_paths(datadir)
    if found_samples:
        config = load_config(majora_token)
        climb_file_server = config['climb_file_server']
        climb_username = config['climb_username']     
        climb_server_conn = ClimbFiles(climb_file_server, climb_username)
        climb_server_conn.create_climb_dir('upload')
        climb_run_directory = os.path.join('upload', run_name)
        climb_server_conn.create_climb_dir(climb_run_directory)
        for sample in found_samples: 
            climb_sample_directory = os.path.join(climb_run_directory, sample['sample_name'])
            climb_server_conn.create_climb_dir(climb_sample_directory)        
            climb_server_conn.put_file(sample['fasta'], climb_sample_directory)
            climb_server_conn.put_file(sample['bam'], climb_sample_directory)
    else: 
        logging.error(f'No samples found in {datadir}')


