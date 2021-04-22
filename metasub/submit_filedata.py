
import logging 
import os 
import re 
import csv 
from climbfiles import ClimbFiles
import json 

def load_config(config="majora.json"):
    config_dict = json.load(open(config))
    return config_dict

def get_file_paths(datadir, platform):
    found_samples = [] 
    output_dir_uploadlist = os.path.join(datadir, 'uploadlist')
    output_dir_blacklist = os.path.join(datadir, 'blacklist')
    uploadlist = None
    blacklist = None
    if os.path.exists(output_dir_uploadlist):
        uploadlist = [x.strip() for x in open(output_dir_uploadlist).readlines()]
    if os.path.exists(output_dir_blacklist):
        blacklist = [x.strip() for x in open(output_dir_blacklist).readlines()]  

    if platform.upper() == 'ILLUMINA':
        output_dir_bams = os.path.join(datadir, 'ncovIllumina_sequenceAnalysis_trimPrimerSequences')
        output_dir_consensus = os.path.join(datadir, 'ncovIllumina_sequenceAnalysis_makeConsensus')
    else:
        output_dir_bams = os.path.join(datadir, "articNcovNanopore_sequenceAnalysisMedaka_articMinIONMedaka")
        output_dir_consensus = os.path.join(datadir, "articNcovNanopore_sequenceAnalysisMedaka_articMinIONMedaka")
    qc_files = [os.path.join(datadir, x) for x in os.listdir(datadir) if x.endswith('.qc.csv')] 
    if qc_files == 1:
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
            found_sample = dict(sample_name=sample_name, fasta=os.path.join(output_dir_consensus, row['fasta']), bam=os.path.join(output_dir_bams, row['bam']))
            if os.path.exists(found_sample['bam']) and os.path.exists(found_sample['fasta']):
                found_samples.append(found_sample)
        return found_samples
    else:
        logging.error(f'Multiple QC files in dir {datadir}')
        return None 

def legacy_submit_filedata(datadir, run_name, platform, majora_token):

    # If a datadir is given, just upload the files. (legacy method)

    found_samples = get_file_paths(datadir, platform)

    config = load_config(majora_token)
    climb_file_server = config['climb_file_server']
    climb_username = config['climb_username']     
    climb_server_conn = ClimbFiles(climb_file_server, climb_username)
    climb_server_conn.create_climb_dir('upload')
    climb_run_directory = os.path.join('upload', run_name)
    for sample in found_samples: 
        climb_sample_directory = os.path.join(climb_run_directory, sample['sample_name'])
        climb_server_conn.create_climb_dir(climb_sample_directory)        
        climb_server_conn.put_file(sample['fasta'], climb_sample_directory)
        climb_server_conn.put_file(sample['bam'], climb_sample_directory)



