
import logging 
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from submit_schema import Samplemeta, SeqMeta
from marshmallow import EXCLUDE
import os 
import csv 

def generate_metasheet(outputdir, datadir, gcredentials, majora_token, sheet_name, submission_sheet_name, library_type, plate_names):
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
        if x.get('library_type') == library_type and x.get('run_name') != '' :
            if x.get('plate') in plate_name_list:
                sample_names.append(x.get('central_sample_id'))
                run_names.append(x.get('run_name'))
                library_names.append(x.get('library_name'))
    run_names = list(set(run_names))                
    if len(run_names) == 1:
        run_name = run_names[0]
        # go to proper table -
        meta_sheet = client.open(sheet_name).sheet1
        all_values = meta_sheet.get_all_records()        
        out_name = os.path.basename(datadir)
        out = os.path.join(outputdir, f'{out_name}.csv')
        all_fields = list(Samplemeta().fields.keys()) + list(SeqMeta().fields.keys())
        out_do = csv.DictWriter(open(out, 'w'), fieldnames=all_fields)
        for x in all_values:
        # and fetch metadata and format for csv upload 
            record =  {k: x[k] for k in all_fields}
            #record.update(x)
            out_do.writerow(record)
    else:
        logging.error('Mulitple run names ')



if __name__ == '__main__':
    generate_metasheet('temp/', '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20210421', 'credentials.json', 'majora.json', 'SARCOV2-Metadata', 'COGUK_submission_status', 'COG', 'COG106,COG107')
    generate_metasheet('temp/', '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20210421-Boat', 'credentials.json', 'majora.json', 'SARCOV2-Metadata', 'COGUK_submission_status', 'Boat', 'COG108')