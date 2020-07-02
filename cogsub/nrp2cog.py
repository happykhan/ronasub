# Converts the meta from nrp to cog uk (all in google sheets)
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import collections
import logging
from marshmallow import Schema, fields, EXCLUDE, pre_load, validate
import csv 
import datetime
import seaborn as sns
import matplotlib.pyplot as plt


class BioMeta(Schema):
    postcode_regex = '([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?)))))'
    get_counties = ['BEDFORDSHIRE', 'BERKSHIRE', 'BRISTOL', 'BUCKINGHAMSHIRE',
         'CAMBRIDGESHIRE', 'CHESHIRE', 'CITY OF LONDON', 'CORNWALL',
          'COUNTY DURHAM', 'CUMBRIA', 'DERBYSHIRE', 'DEVON', 'DORSET',
           'EAST RIDING OF YORKSHIRE', 'EAST SUSSEX', 'ESSEX', 'GLOUCESTERSHIRE', 'GREATER LONDON', 'GREATER MANCHESTER',
           'HAMPSHIRE', 'HEREFORDSHIRE', 'HERTFORDSHIRE', 'ISLE OF WIGHT', 'KENT', 'LANCASHIRE', 'LEICESTERSHIRE',
           'LINCOLNSHIRE', 'MERSEYSIDE', 'NORFOLK', 'NORTH YORKSHIRE', 'NORTHAMPTONSHIRE', 'NORTHUMBERLAND', 'NOTTINGHAMSHIRE',
           'OXFORDSHIRE', 'RUTLAND', 'SHROPSHIRE','SOMERSET','SOUTH YORKSHIRE','STAFFORDSHIRE','SUFFOLK','SURREY',
           'TYNE AND WEAR','WARWICKSHIRE','WEST MIDLANDS','WEST SUSSEX','WEST YORKSHIRE','WILTSHIRE','WORCESTERSHIRE', 'MIDDLESEX']

    central_sample_id = fields.Str(data_key="COG Sample ID", required=True, validate=validate.Regexp("^NORW-[a-zA-Z0-9]{5}$"))
    biosample_source_id = fields.Str(data_key="NNUH Sample ID", required=True)
    adm1 = fields.Str(missing="UK-ENG")
    adm2 = fields.Str(data_key="County", validate=validate.OneOf(get_counties))
    source_age = fields.Integer(data_key="Age", validate=validate.Range(min=0, max=120))
    source_sex = fields.Str(data_key="Sex", validate=validate.OneOf(['M','F']))
    received_date = fields.Str()
    collection_date = fields.Date(data_key="Collection date")
    sample_type_collected = fields.Str(data_key="Source", validate=validate.OneOf(["dry swab", "swab", "sputum", "aspirate"]))
    swab_site = fields.Str(data_key="Body site", validate=validate.OneOf(["nose", "throat", "nose-throat", "endotracheal", "rectal"]))
    collecting_org = fields.Str(data_key="Collecting organisation")
    library_name = fields.Str()
    library_seq_kit = fields.Str(missing='Nextera')
    library_seq_protocol = fields.Str(missing='Nextera LITE')
    library_layout_config = fields.Str(missing='PAIRED')
    library_selection = fields.Str(missing='PCR')
    library_source = fields.Str(missing='VIRAL_RNA')
    library_strategy = fields.Str(missing='AMPLICON')
    library_primers = fields.Integer(missing=3)
    library_protocol = fields.Str(missing='ARTICv2')
    run_name = fields.Str()
    previous_runs = fields.Str()
    instrument_make = fields.Str(missing='ILLUMINA')
    instrument_model = fields.Str(missing='NextSeq 500')
    adm2_private = fields.Str(data_key="Outer Postcode", validate=validate.Regexp(postcode_regex))
    date_sequenced = fields.Str()
    repeat_sample_id = fields.Str(data_key="Repeat Sample ID")
    is_icu_patient = fields.Str(data_key="ICU admission", validate=validate.OneOf(['Y','N', 'Unknown']))
   # ct_value = fields.Str(data_key='PCR Ct value')

    @pre_load
    def clean_up(self, in_data, **kwargs):
        if in_data.get('Collecting organisaton'):
            in_data['Collecting organisation'] = in_data.get('Collecting organisaton')
        for k,v in dict(in_data).items():
            if v in ['', 'to check'] :
                in_data.pop(k)        
            elif k in ['County', 'Collecting organisation', 'Outer Postcode'] and v.upper() in ['NOT AVAILABLE', 'UNKNOWN', 'NO ADDRESS', 'NO POST CODE']:
                in_data.pop(k)
            elif k in ['Sex'] and v.upper() in ['U', 'N']:
                in_data.pop(k)                
            elif k in ['ICU admission'] and v.upper() in ['U', 'UKNOWN']:       
                in_data.pop(k)         
            elif isinstance(v, str):
                    in_data[k] = v.strip()
        if in_data.get('Source','').lower() in ['bronchial washings','bronchial washing']:
            in_data['Source'] = 'aspirate'
        if in_data.get("County"):
            in_data["County"] = in_data["County"].upper()
        if in_data.get("County", '').upper() in ['CAMBS', 'CAMBRIDESHIRE', 'CAMBRIDGE', 'CAMBRIDGSHIRE']:
            in_data["County"] = 'CAMBRIDGESHIRE'
        if in_data.get("County", '').upper() == 'LINC':
            in_data["County"] = 'LINCOLNSHIRE'
        if in_data.get("County", '').upper() == 'LONDON':
            in_data["County"] = 'GREATER LONDON'            
        if in_data.get("County", '').upper() == 'COLCHESTER':
            in_data["County"] = 'ESSEX'            
        if in_data.get("Source"):
            in_data["Source"] = in_data["Source"].lower()            
        if in_data.get('Body site'):
            if in_data.get('Body site').lower() in ['throat/nose', 'nose/throat']:
                in_data['Body site'] = 'nose-throat'
            elif in_data.get('Body site').lower() in ['lung', "tracheostomy"]:
                in_data['Body site'] = 'endotracheal'
            elif in_data.get('Body site').lower() in ['mouth', 'throat/swab']:
                in_data['Body site'] = 'throat'                
            else:
                in_data['Body site'] = in_data.get('Body site').lower()
        if in_data.get('ICU admission', '').lower() in ['yes']:
            in_data['ICU admission'] = 'Y'
        if in_data.get('ICU admission', '').lower() in ['no']:
            in_data['ICU admission'] = 'N'   
        if  in_data.get('Collection date'):
            in_data['Collection date'] = self.handle_dates(in_data['Collection date'])            
        return in_data

    def handle_dates(self, date_string):
        try:
            datetime.datetime.strptime(date_string, '%Y-%m-%d')
            # String is fine, return itself. 
            return date_string
        except ValueError:
            try:
                datetime_obj = datetime.datetime.strptime(date_string, '%d/%m/%Y')
                return datetime_obj.strftime('%Y-%m-%d')
            except ValueError:
                try:
                    datetime_obj = datetime.datetime.strptime(date_string, '%d.%m.%Y')
                    return datetime_obj.strftime('%Y-%m-%d')
                except ValueError:
                    raise

        

class CtMeta(Schema):
    ct_1_ct_value = fields.Float()
    ct_1_test_kit = fields.Str()
    ct_1_test_platform = fields.Str()
    ct_1_test_target = fields.Str()
    ct_2_ct_value = fields.Float()
    ct_2_test_kit = fields.Str()
    ct_2_test_platform = fields.Str()
    ct_2_test_target = fields.Str()    


    @pre_load
    def clean_up(self, in_data, **kwargs):
        for k,v in dict(in_data).items():
            if v in ['', 'to check',  '#VALUE!', '-', 'N/A'] :
                in_data.pop(k)        
            elif isinstance(v, str):
                    in_data[k] = v.strip()
        return in_data

class lineageMeta(Schema):
    uk_lineage = fields.Str(data_key='peroba_uk_lineage')
    lineage = fields.Str(data_key='peroba_lineage')
    phylotype = fields.Str(data_key='peroba_phylotype')
    special_lineage = fields.Str(data_key='peroba_special_lineage')

    @pre_load
    def clean_up(self, in_data, **kwargs):
        for k,v in dict(in_data).items():
            if v in [''] :
                in_data.pop(k)        
            elif isinstance(v, str):
                    in_data[k] = v.strip()
        return in_data

def get_bio_metadata(client, sheet_name='COG_UK_Metadata_QIB_Deidentified'):

    meta_data = {}
    for sheet in client.open(sheet_name).worksheets():
        all_values = sheet.get_all_records()
        for x in all_values:
            if x['COG Sample ID'].startswith('NORW'):
                record = BioMeta(unknown = EXCLUDE).load(x)
                meta_data[record['central_sample_id']] = record

    return meta_data

def get_ct_metadata(client, sheet_name='cov-ct'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    cts = {}
    for x in all_values:
        if x['central_sample_id'].startswith('NORW'):
            record = CtMeta(unknown = EXCLUDE).load(x)
            cts[x['central_sample_id']] = record
    return cts


def update_our_meta(new_data, client, sheet_name='SARCOV2-Metadata', force_update = True):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)
    no_bio_meta_data = set(row_position) - set([x['central_sample_id'] for x in new_data.values()])
    print('NO BIO META found for ' + ','.join(no_bio_meta_data))
    cells_to_update = []
    duplicates = [item for item, count in collections.Counter(row_position).items() if count > 1]
    if duplicates:
        print('Following records are duplicated in master sheet:' + ','.join(duplicates))
    missing_rows_names = list(set([x['central_sample_id'] for x in new_data.values()]) - set(row_position) )
    if missing_rows_names:
        print('PLEASE ADD THESE ROWS\n' + '\n'.join(missing_rows_names))
    for x in all_values:
        bio_metadata = new_data.get(x['central_sample_id'])
        if bio_metadata:
            for key, value in bio_metadata.items():
                # Handle date.
                if key == 'collection_date':
                    value =  value.strftime('%Y-%m-%d')                
                if x[key] == '':
                    # look up row position
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index(key)+1, value=value))
                elif x[key] != value:
                    print(f"{x['central_sample_id']} : {key} != \"{value}\" It currently is \"{x[key]}\"")
                    if force_update:
                        cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index(key)+1, value=value))
        else:
            print('NO METADATA IN BIO REPOSITORY FOR ' + x['central_sample_id'])
    if cells_to_update:
        print('Updating values')
        sheet.update_cells(cells_to_update)
    else:
        print('All values sync. Nothing to update')


def update_ct_meta(new_data, client, sheet_name='SARCOV2-Metadata'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)
    no_ct_meta_data = set(row_position) - set(new_data.keys())
    print('No CT META found for ' + ','.join(no_ct_meta_data))
    cells_to_update = []
    print('Following records are duplicated in master sheet:' + ','.join([item for item, count in collections.Counter(row_position).items() if count > 1]))
    print('PLEASE ADD THESE ROWS\n' + '\n'.join(list(set(new_data.keys()) - set(row_position) )))
    for x in all_values:
        ct_metadata = new_data.get(x['central_sample_id'])
        if ct_metadata:
            if ct_metadata.get('ct_1_ct_value'):
                if[x['ct_1_ct_value']] != ct_metadata['ct_1_ct_value']:
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_1_ct_value')+1, value=ct_metadata['ct_1_ct_value']))
                if ct_metadata.get('ct_1_test_platform') == "AusDiagnostics":
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_1_test_platform')+1, value='AUSDIAGNOSTICS'))
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_1_test_kit')+1, value='AUSDIAGNOSTICS'))
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_1_test_target')+1, value='ORF1AB'))
                if ct_metadata.get('ct_1_test_platform') == "Roche Cobas":
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_1_test_platform')+1, value='ROCHE_COBAS'))
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_1_test_kit')+1, value='ROCHE'))
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_1_test_target')+1, value='RDRP'))                    

            if ct_metadata.get('ct_2_ct_value'):
                if[x['ct_2_ct_value']] != ct_metadata['ct_2_ct_value']:
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_2_ct_value')+1, value=ct_metadata['ct_2_ct_value']))
                if ct_metadata.get('ct_2_test_platform') == "AusDiagnostics":
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_2_test_platform')+1, value='AUSDIAGNOSTICS'))
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_2_test_kit')+1, value='AUSDIAGNOSTICS'))
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_2_test_target')+1, value='ORF8'))                    
                if ct_metadata.get('ct_2_test_platform') == "Roche Cobas":
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_2_test_platform')+1, value='ROCHE_COBAS'))
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_2_test_kit')+1, value='ROCHE'))
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index('ct_2_test_target')+1, value='E'))                                        
        else:
            print('NO CT METADATA  FOR ' + x['central_sample_id'])
    if cells_to_update:
        print('Updating values')
        sheet.update_cells(cells_to_update)
    else:
        print('All values sync. Nothing to update')


def get_lineage_metadata(client, sheet_name='peroba'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    lineage = {}
    for x in all_values:
        if x['central_sample_id'].startswith('NORW'):
            record = lineageMeta(unknown = EXCLUDE).load(x)
            lineage[x['central_sample_id']] = record
    return lineage
    
def update_lineage_meta(new_data, client, sheet_name='SARCOV2-Metadata'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)
    no_lineage_meta_data = set(row_position) - set(new_data.keys())
    print('No lineage META found for ' + '\n'.join(no_lineage_meta_data))
    cells_to_update = []
    print('Following records are duplicated in master sheet:' + ','.join([item for item, count in collections.Counter(row_position).items() if count > 1]))
    print('PLEASE ADD THESE ROWS\n' + '\n'.join(list(set(new_data.keys()) - set(row_position) )))
    for x in all_values:
        lineage_metadata = new_data.get(x['central_sample_id'])
        if lineage_metadata:
            for key, value in lineage_metadata.items():
                cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index(key)+1, value=value))
        else:
            print('NO LINEAGE METADATA  FOR ' + x['central_sample_id'])
    if cells_to_update:
        print('Updating values')
        sheet.update_cells(cells_to_update)
    else:
        print('All values sync. Nothing to update')


def get_surv_metadata(client, sheet_name='Sarcov_Sampling_info'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    surv = {}
    surv_counts = {}
    for x in all_values:
        surv_counts[str(x['run_date']) + '-' + x['region'].upper()] = dict(total_count = x['sample_count'], current_count=0)
        for y in x['org_list'].split(','):
            surv[y.strip()] = x['region'].upper()
    return surv, surv_counts

def update_surv_meta(surv, surv_counts, client, sheet_name='SARCOV2-Metadata'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    current_count = {}
    for x in all_values:
        this_org = x['collecting_org']
        this_region = surv.get(this_org)
        if this_region and x['run_name']:
            region_key = x['run_name'][0:6] + '-' +  this_region.upper()
            if surv_counts.get(region_key):
                if x.get('is_surveillance') == 'Y':
                    surv_counts[region_key]['current_count'] += 1 
    for y, x in surv_counts.items():
        if x['current_count'] != x['total_count']:
            print(f'MISMATCH IN SAMPLING. There should be {x["total_count"]} YES for {y}. Actual value is {x["current_count"]}')
            valid_orgs = [k for k,v in surv.items() if y.split('-')[1] == v]
            print(f'Valid orgs are ' + ','.join(valid_orgs))
    print(surv_counts)

import re

def common_member(a, b): 
    a_set = set(a) 
    b_set = set(b) 
    if (a_set & b_set): 
        return True 
    else: 
        return False

def update_patient_id(client, sheet_name='SARCOV2-Metadata'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    linked = []
    lineage_lookup = {} 
    qc_lookup = {}
    date_lookup = {}
    uniq_biosample = [] 
    for x in all_values:
        if not x['central_sample_id'].endswith('duplicate'):
            date_lookup[x['central_sample_id']] = x['collection_date']
            lineage_lookup[x['central_sample_id']] = x['uk_lineage']
            qc_lookup[x['central_sample_id']] = x['Basic QC']        

            if len(x['biosample_source_id']) > 1 :
                uniq_biosample.append(x['biosample_source_id'])
                linked_values = [] 
                linked_values += re.findall('(D,20.\d+)', x['biosample_source_id'])
                linked_values += re.findall('(20C\d{6})', x['biosample_source_id'])
                linked_values += re.findall('(D20.\d+)', x['biosample_source_id'])
                linked_values += re.findall('(D,20.\d+)', x['repeat_sample_id'])
                linked_values += re.findall('(20C\d{6})', x['repeat_sample_id'])
                linked_values += re.findall('(D20.\d+)', x['repeat_sample_id'])
                linked_values.append(x['central_sample_id'])
                added = False
                for exist in linked:
                    if common_member(linked_values, exist):
                        exist += linked_values
                        added = True
                if not added:
                    linked.append(linked_values)
    cells_to_update = [] 
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)                         
    for link in linked:
        ids = [x for x in link if x.startswith('NORW')]
        min_id = sorted(ids)[0]  
        if len(ids) > 1:
            for id in ids: 
                cells_to_update.append(gspread.models.Cell(row=row_position.index(id)+1, col=column_position.index('patient_group')+1, value=min_id))
    if cells_to_update:
        print('Updating values')
        sheet.update_cells(cells_to_update)
    all_values = sheet.get_all_records()
    # Reports 
    with open('patient_groups.txt', 'w') as pat_out:
        pat_out.write('PATIENT_GROUP\tCOGID\tDATE\tLINEAGE\tBASICQC\n')
        group_sum = {}
        for x in all_values:
            pat = x['patient_group']
            cog_id = x['central_sample_id']
            if pat:
                pat_out.write(f'{pat}\t{cog_id}\t{x["collection_date"]}\t{x["uk_lineage"]}\t{x["Basic QC"]}\n')
                if group_sum.get(pat):
                    group_sum[pat]['count'] += 1
                    group_sum[pat]['date_list'].append(x['collection_date'])
                else:
                    group_sum[pat] = dict(count=1, date_list = [x['collection_date']])
        total_multiple_samples = sum([x['count'] for x in group_sum.values()])
        multiple_dates = len([x for x,y in group_sum.items() if len(list(set(y['date_list']))) > 1])
        print(f'TOTAL SAMPLES THAT ARE LINKED: {total_multiple_samples}')
        print(f'TOTAL GROUPS THAT ARE LINKED: {len(group_sum)}')
        print(f'TOTAL GROUPS THAT ARE LINKED (with different dates): {multiple_dates}')
        from itertools import groupby
        group_counts = sorted([x['count'] for x in group_sum.values() if x['count'] > 1])
        group_counted = {key: len(list(group)) for key, group in groupby(group_counts)}
        print('linked_samples_group\tFrequency')
        plt.style.use('ggplot')

        plt.bar(group_counted.keys(), group_counted.values(), color='green')
        plt.xlabel('No. of samples in a group')
        plt.ylabel('Count')
        plt.savefig('linked_samples_group.png')
        plt.savefig('linked_samples_group.svg')
        for x,y in group_counted.items():
            print(f'{x}\t{y}')
        group_date_count = sorted([len(list(set(y['date_list']))) for x,y in group_sum.items() if len(list(set(y['date_list']))) > 1])
        group_counted = {key: len(list(group)) for key, group in groupby(group_date_count)}
        print('linked_date_group\tFrequency')
        for x,y in group_counted.items():
            print(f'{x}\t{y}')
        plt.bar(group_counted.keys(), group_counted.values(), color='green')
        plt.xlabel('No. of samples in a group')
        plt.ylabel('Count')
        plt.savefig('linked_date_group.png')
        plt.savefig('linked_date_group.svg')        

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('cogsub/credentials.json', scope)
client = gspread.authorize(creds)
logging.basicConfig(level=logging.DEBUG)

# Update surveliannce 
#new_dict, surv_counts = get_surv_metadata(client)
#update_surv_meta(new_dict, surv_counts, client)


# Update Lineage
new_dict = get_lineage_metadata(client)
update_lineage_meta(new_dict, client)

# Update CT.
new_dict = get_ct_metadata(client)
update_ct_meta(new_dict, client)

new_dict = get_bio_metadata(client)
update_our_meta(new_dict, client, force_update = False)

# Create Patients field
update_patient_id(client)