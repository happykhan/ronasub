import gspread
import logging 
import paramiko
import os 
import csv

class PheFiles():

    def __init__(self, file_server, username, key):
        ssh = paramiko.SSHClient()
        the_key = paramiko.RSAKey.from_private_key_file(key)
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(file_server, username=username, pkey=the_key, port=443)
        self.sftp = ssh.open_sftp() 

    def create_dir(self, dir_name):
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


def export_phe(client, out_dir, sheet_name, file_server, username, key):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    load = { }
    logging.info('Reading values from master table')
    for x in all_values:
        if len(x['uk_lineage']) > 2 : 
            date = ''
            if len(x['collection_date']) > 3 :
                date = x['collection_date']
            else:
                date = x['received_date']
            if not x['central_sample_id'].endswith('duplicate'):
                load[x['central_sample_id']] = { "cog_id": x['central_sample_id'], "biosample_source_id": x["biosample_source_id"]}
    out_file = os.path.join(out_dir, 'NORW-cog2labid.csv') 
    
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    logging.info('Writing to temp file')
    with open(out_file, 'w') as f:
        temp_out_file = csv.DictWriter(f, fieldnames=list(load.values())[0].keys())
        temp_out_file.writeheader()
        temp_out_file.writerows(load.values())
    logging.info('Uploading to server')        
    server = PheFiles(file_server, username, key)
    server.put_file(out_file, 'upload')
    logging.info('Done')                    
