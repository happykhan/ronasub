"""
new_data imports some new info to master table 

Requires login for google sheets

### CHANGE LOG ### 
2020-08-17 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build - split from dirty scripts
"""
import meta
from nrp_util import get_google_session
import logging
import time 
import sys 
import argparse
import gspread
import collections
import re 

epi = "Licence: " + meta.__licence__ +  " by " +meta.__author__ + " <" +meta.__author_email__ + ">"
logging.basicConfig()
log = logging.getLogger()


def get_new_metadata(client, sheet_name):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    new_data = {}
    for x in all_values:
        # Strip the useless suffix. 
        new_data[x['lab_id']] = x

    return new_data

def clean_lab_ids(client, new_dict, sheet_name):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)    
    new_data = {}
    cells_to_update = []
    for x in all_values:
        # Check lab id is ok . 
        id = x.get('biosample_source_id')
        if id == '':
            continue
        if re.match("[DK],20.\d{7}.\w|20C\d{6}|M\d{8}", id):
            cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index("clean_biosample_source_id")+1, value=id))
        else:
            # get the good stuff
            bad = re.match("([DK]),?.?20.0*(\d+).?(\w?)",id)
            real_id = ''
            if bad:

                real_id = f'{bad.group(1)},20.{bad.group(2).zfill(7)}'        
                if bad.group(3) != "":
                    real_id += f'.{bad.group(3)}'
            if real_id == '':
                print('bad lab id ' + id)
            else:
                # Add suffix Letter back 
                if not new_dict.get(real_id):
                    suffix = [x for x in  list(new_dict.keys()) if x.startswith(real_id)] 
                    if suffix: 
                        real_id = suffix[0] 
                cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index("clean_biosample_source_id")+1, value=real_id))
    if cells_to_update:
        logging.info('Updating values')
        sheet.update_cells(cells_to_update)
    else:
        logging.info('All values sync. Nothing to update')  


def update_new_meta(new_data, client, sheet_name='SARCOV2-Metadata'):
    sheet = client.open(sheet_name).sheet1
    sheet = client.open(sheet_name).worksheet('Original_QIB')
    all_values = sheet.get_all_records()
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)
    cells_to_update = []
    logging.info('Following records are duplicated in master sheet:' + ','.join([item for item, count in collections.Counter(row_position).items() if count > 1]))
    list_of_ids = [] 
    all_ids = [] 
    for x in all_values:

        real_id = x['clean_biosample_source_id']
        if real_id == '':
            continue        
        new_metadata = new_data.get(real_id)
        # maybe the lab id is the short hand. 
        all_ids.append(real_id)
        if len(x.get('run_name')) > 3: 
            list_of_ids.append(real_id)
        if new_metadata:
            for k, v in new_metadata.items():
                if k not in ['sequenced', 'lab_id']:
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('clean_' + k )+1, value=v))
        else:
            logging.info('No value for ' + real_id)    
    table(list_of_ids, all_ids)
    if cells_to_update:
        logging.info('Updating values')
        sheet.update_cells(cells_to_update)
    else:
        logging.info('All values sync. Nothing to update')    

def table(list_of_ids, all_ids):
    import csv
    yes = [] 
    for x in csv.DictReader(open('clean_table.csv'), dialect=csv.excel):
        x_id = re.match('.+20.(\d+).*', x['sample'])
        find_close_match = 0
        for id in all_ids:
            id_id = re.match('.+20.(\d+).*', id)
            if id_id and x_id: 
                if int(id_id.group(1)) == int(x_id.group(1)):
                    find_close_match += 1
        if find_close_match > 0 :
            x['close'] =  x['sample']
        else:
            x['close'] = False


        if x['sample'] in all_ids:
            x['seen'] = True
        else:
            x['seen'] = False
        if x['sample'] in list_of_ids:
            x['sequenced'] = True
        else:
            x['sequenced'] = False
        
        

        yes.append(x)
    with open('clean_table_seq.tsv', 'w') as outout:            
        out = csv.DictWriter(outout, fieldnames=yes[0].keys(), dialect=csv.excel_tab)            
        out.writeheader()
        out.writerows(yes)



def main(args):
    client = get_google_session(args.gcredentials)
    new_dict = get_new_metadata(client, sheet_name=args.newdata)
    clean_lab_ids(client, new_dict, sheet_name=args.maindata)
    update_new_meta(new_dict, client, sheet_name=args.maindata)   


if __name__ == '__main__':
    start_time = time.time()
    log.setLevel(logging.INFO)
    desc = __doc__.split('\n\n')[0].strip()
    parser = argparse.ArgumentParser(description=desc,epilog=epi)
    parser.add_argument ('-v', '--verbose', action='store_true', default=False, help='verbose output')
    parser.add_argument('--version', action='version', version='%(prog)s ' + meta.__version__)
    parser.add_argument('--gcredentials', action='store', default='credentials.json', help='Path to Google Sheets API credentials (JSON)')
    parser.add_argument('--maindata', action='store', default='SARCOV2-Metadata - Freeze 2002-09-01',  help='Name of Master Table in Google sheets')
    parser.add_argument('--worksheet', action='store', default='Original_QIB',  help='Name of worksheet')    
    parser.add_argument('--newdata', action='store', default='cleaned_newnew',  help='Name of NEW Table in Google sheets')
    args = parser.parse_args()
    if args.verbose: 
        log.setLevel(logging.DEBUG)
        log.debug( "Executing @ %s\n"  %time.asctime())    
    main(args)
    if args.verbose: 
        log.debug("Ended @ %s\n"  %time.asctime())
        log.debug('total time in minutes: %d\n' %((time.time() - start_time) / 60.0))
    sys.exit(0)