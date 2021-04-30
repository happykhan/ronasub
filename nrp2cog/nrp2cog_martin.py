"""
nrp2cog converts the meta from nrp to cog uk (all in google sheets)

Requires login for google sheets

### CHANGE LOG ### 
2020-08-17 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build - split from dirty scripts
"""
import logging
import time
import argparse
import sys
import meta
import csv
import gspread

from nrp_util import get_google_session
from update_surv import get_surv_metadata, update_surv_meta
from export_lineage import update_pub_info
from export_phe import export_phe
from update_lineage import get_lineage_metadata, update_lineage_meta
from ct_update import get_ct_metadata, update_ct_meta
from update_metadata import get_bio_metadata, update_our_meta, update_patient_id
import json
from update_civet import get_civet_metadata, update_civet_meta 

epi = "Licence: " + meta.__licence__ +  " by " +meta.__author__ + " <" +meta.__author_email__ + ">"
logging.basicConfig()
log = logging.getLogger()


def update_surv_option(args):
    # TODO: Add to options
    # Update surveliannce 
    client = get_google_session(args.gcredentials)
    new_dict, surv_counts = get_surv_metadata(client)
    update_surv_meta(new_dict, surv_counts, client)


def export_lineage_option(args):

    # Build def info:
    client = get_google_session(args.gcredentials)
    update_pub_info(client)

def export_phe_option(args):    
    client = get_google_session(args.gcredentials)
    config = json.load(open(args.config))
    export_phe(client, config['temp_dir'], config['sheet_name'], config['export_server'], config['export_username'], config['key_location'])


def update_lineage_option(args):

    client = get_google_session(args.gcredentials)
    new_dict = get_lineage_metadata(client, sheet_name=args.lineagedata)
    update_lineage_meta(new_dict, client)    

def update_metadata_option(args):

    client = get_google_session(args.gcredentials)
    new_dict, errors = get_bio_metadata(client)
    update_our_meta(new_dict, client, force_update = False)
    # Create Patients field
    update_patient_id(client)

def update_ct_option(args):

    client = get_google_session(args.gcredentials)
    new_dict, errors = get_ct_metadata(client, sheet_name=args.ctdata)
    update_ct_meta(new_dict, client, sheet_name=args.maindata)

def update_civet_option(args):

    client = get_google_session(args.gcredentials)
    new_dict = get_civet_metadata(args.rundir)
    if new_dict:
        update_civet_meta(new_dict, client, sheet_name=args.maindata)

def update_sample_meta(args):
    client = get_google_session(args.gcredentials)
    sheet = client.open('COGUK_submission_status').get_worksheet(0) # Index from 0, get the second sheet.
    all_values = sheet.get_all_records()
    
    sample2keyValues=dict()
    for key2value in all_values:
        key = str(key2value['central_sample_id']) + str(key2value['library_name'])
        sample2keyValues[key] = key2value

    # Then iterate through the cells for the row and update accordingly
    
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)

    new_rows=list()
    cells_to_update=list()
    first_row=True
    index2key=dict()
    with open('../metasub/gather_plates.csv') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            if first_row==True:
                for i in range(0,len(row)):
                    index2key[i] = row[i]
                first_row=False
            else:
                key = str(row[0]) + str(row[1])
                if key not in sample2keyValues.keys(): # Add this new row
                    new_rows.append(row)
                else: # This row already exists, check if there is anything to update...
                    old_data = sample2keyValues[key]

                    new_data=dict()
                    for i in range(0,len(row)):
                        if i<len(index2key):
                            new_data[index2key[i]] = row[i]

                    for old_key in old_data.keys():
                        if old_key in new_data.keys() and not str(old_data[old_key])==str(new_data[old_key]):
                            changed=True
                            if isinstance(old_data[old_key], int) and not new_data[old_key]=='':
                                changed = int(old_data[old_key])!=int(new_data[old_key])
                                
                            if changed==True and (isinstance(old_data[old_key], str) and len(old_data[old_key])==0): # Only update blank cells
                                print('Change found for sample [' + key + '] with field [' + old_key + '] [' + str(old_data[old_key]) + '] -> [' + str(new_data[old_key]) + ']')
                                cells_to_update.append(gspread.models.Cell(row=list(sample2keyValues.keys()).index(key)+2, col=list(old_data.keys()).index(old_key)+1, value=new_data[old_key]))

    if len(new_rows)==0:  print('No new rows were added')
    else: print('Adding ' + str(len(new_rows)) + ' rows')

    # Currently the fields are slightly different...delete and push again if the columns are correct
    
    #new_rows=[['new row 1','column2'],['new row 2'],['new row 3']]
    # Now try updating that sheet...
    sheet.resize(len(row_position)) # Trim any whitespace at the end of the sheet...
#    values = [[x] for x in missing_rows_names]
    sheet.append_rows(new_rows)

    if cells_to_update:
        print('Updating values')
        sheet.update_cells(cells_to_update)
    
    all_values = sheet.get_all_records()
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)

    print('Update submission sheet has finished')


def summarise_plates(args):
    client = get_google_session(args.gcredentials)
    sheet = client.open('COGUK_submission_status').get_worksheet(0) # Index from 0, get the second sheet.
    all_values = sheet.get_all_records()

    print('plate_name\tsequencing_date\tnumber_of_samples\tpercentage_passed\tpercentage_hc_passed')

    plate2date=dict()
    plate2number_of_samples=dict()
    plate2passes=dict()
    plate2hc_passes=dict()
    
    for key2value in all_values:
        plate_name = str(key2value['plate'])
        sequencing_date = str(key2value['sequencing_date'])
        basic_qc = str(key2value['basic_qc'])
        high_quality_qc = str(key2value['high_quality_qc'])

        if not plate_name in plate2date.keys(): plate2date[plate_name] = set()
        plate2date[plate_name].add(sequencing_date)
        
        if not plate_name in plate2number_of_samples.keys(): plate2number_of_samples[plate_name]=1
        else:  plate2number_of_samples[plate_name]=plate2number_of_samples[plate_name]+1
        
        if basic_qc=='True':
            if not plate_name in plate2passes.keys(): plate2passes[plate_name]=1
            else:  plate2passes[plate_name]=plate2passes[plate_name]+1

        if high_quality_qc=='True':
            if not plate_name in plate2hc_passes.keys(): plate2hc_passes[plate_name]=1
            else:  plate2hc_passes[plate_name]=plate2hc_passes[plate_name]+1

    for plate_name in sorted(plate2date.keys()):
        passes=0
        if plate_name in plate2passes.keys(): passes = plate2passes[plate_name]
        percentage_passes = (passes*100)/plate2number_of_samples[plate_name]

        hc_passes=0
        if plate_name in plate2hc_passes.keys(): hc_passes = plate2hc_passes[plate_name]
        hc_percentage_passes = (hc_passes*100)/plate2number_of_samples[plate_name]

        if plate2number_of_samples[plate_name]>1: print(plate_name + '\t' + str(list(plate2date[plate_name])) + '\t' + str(plate2number_of_samples[plate_name]) + '\t' + "{:.2f}".format(percentage_passes) + '%\t' + "{:.2f}".format(hc_percentage_passes) + '%')
    
if __name__ == '__main__':
    start_time = time.time()
    log.setLevel(logging.INFO)
    desc = __doc__.split('\n\n')[0].strip()
    parser = argparse.ArgumentParser(description=desc,epilog=epi)
    subparsers = parser.add_subparsers(help='commands')
    parser.add_argument ('-v', '--verbose', action='store_true', default=False, help='verbose output')
    parser.add_argument('--version', action='version', version='%(prog)s ' + meta.__version__)
    parser.add_argument('--gcredentials', action='store', default='credentials.json', help='Path to Google Sheets API credentials (JSON)')
    parser.add_argument('--maindata', action='store', default='SARCOV2-Metadata',  help='Name of Master Table in Google sheets')

    meta_parser = subparsers.add_parser('update_sample_metadata', help='Update sample metadata')
    meta_parser.set_defaults(func=update_sample_meta)

    meta_parser = subparsers.add_parser('summarise_plates', help='Summarise plates')
    meta_parser.set_defaults(func=summarise_plates)
    
    args = parser.parse_args()
    if args.verbose: 
        log.setLevel(logging.DEBUG)
        log.debug( "Executing @ %s\n"  %time.asctime())    
    if hasattr(args, 'func'):
        args.func(args)
    else: 
        parser.print_help()
    if args.verbose: 
        log.debug("Ended @ %s\n"  %time.asctime())
        log.debug('total time in minutes: %d\n' %((time.time() - start_time) / 60.0))
    sys.exit(0)
