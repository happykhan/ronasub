
import logging 
import os 

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


def submit_filedata(sheet_name, submission_sheet, gcredentials, datadir):

    if datadir: 
        # If a datadir is given, just upload the files. (legacy method)
        illumina = True
        if illumina:
            found_samples = read_illumina_dirs
        else:
            found_samples = read_ont_dirs
        
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
        pass
        # Open submission sheet, select run name or library name 
        





