
import logging 
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from submit_schema import Samplemeta, SeqMeta
from marshmallow import EXCLUDE
import os 
import json
import csv 
from datetime import datetime

def generate_metasheet(outputdir, datadir, gcredentials, sheet_name, submission_sheet_name, library_type, plate_names, sample_only=False, run_name=None):
    plate_name_list = plate_names.split(',')

    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(gcredentials, scope)
    client = gspread.authorize(creds)

    # Use submission status to fetch output dirs & samples to use 
    submission_sheet = client.open(submission_sheet_name).worksheet('Sheet2')
    all_values = submission_sheet.get_all_records()
    run_names = [] 
    sample_names = []
    library_names = []
    for x in all_values:
        if x.get('library_type') == library_type and x.get('run_name') != ''  and x.get('qc_pass') == '':
            if str(x.get('plate')) in plate_name_list:
                if run_name:
                    if run_name == x.get('run_name'):
                        sample_names.append(x.get('central_sample_id'))
                        run_names.append(x.get('run_name'))
                        library_names.append(x.get('library_name'))                
                else:
                    sample_names.append(x.get('central_sample_id'))
                    run_names.append(x.get('run_name'))
                    library_names.append(x.get('library_name'))
    run_names = list(set(run_names))                
    if len(run_names) == 1 or sample_only:
        if not sample_only:
            run_name = run_names[0]
            library_name = library_names[0]
        # go to proper table -
        meta_sheet = client.open(sheet_name).sheet1
        all_values = meta_sheet.get_all_records()       
        if datadir: 
            out_name = os.path.basename(datadir)
        else:
            out_name = 'generated_sheet'
        out = os.path.join(outputdir, f'{out_name}.csv')
        record = Samplemeta(unknown = EXCLUDE).load(x)
        if sample_only:
            all_fields = [x.strip() for x in open('metasub/sample_only_fields').readlines()] 
        else:
            all_fields = [x.strip() for x in open('metasub/sample_and_library_fields').readlines()] 
        out_do = csv.DictWriter(open(out, 'w'), fieldnames=all_fields)
        out_do.writeheader()
        for x in all_values:
            if x.get('central_sample_id') in sample_names:
        # and fetch metadata and format for csv upload 
                if sheet_name == 'SARSCOV2-REACT-Metadata':
                    if not x.get('is_surveillance'):
                        x['is_surveillance'] = 'N'
                    if not x.get('received_date') and not sample_only:
                        x['received_date'] =  datetime.strftime(datetime.strptime(run_name.split('_')[0], '%y%m%d'), '%Y-%m-%d')
                    if not x.get('collecting_org'):
                        x['collecting_org'] =  'REACT'
                record = Samplemeta(unknown = EXCLUDE).load(x)
                clean_record = json.loads(json.dumps(record, default=str))
                if not sample_only:
                    x['run_name'] = run_name
                    x['library_name'] = library_name
                    lib_record = SeqMeta(unknown = EXCLUDE).load(x)
                    clean_record.update(json.loads(json.dumps(lib_record, default=str)))
                out_do.writerow(clean_record)
        return out
    else:
        logging.error('Mulitple run names ')
        return None



if __name__ == '__main__':
    
    generate_metasheet('temp/', '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20210428', 'credentials.json', 'SARCOV2-Metadata', 'COGUK_submission_status', 'COG', 'COG109', sample_only=True)
