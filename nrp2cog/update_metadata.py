from marshmallow import EXCLUDE
from marshmallow import ValidationError
import re 
import matplotlib.pyplot as plt
from nrpschema import BioMeta
import collections
import gspread
import logging
import os 

def common_member(a, b): 
    a_set = set(a) 
    b_set = set(b) 
    if (a_set & b_set): 
        return True 
    else: 
        return False


def get_bio_metadata(client, sheet_name='COG-UK Raw Metadata'):

    meta_data = {}
    errors = {}
    for sheet in client.open(sheet_name).worksheets():
        all_values = sheet.get_all_records()
        for x in all_values:
            if x['COG Sample ID'].startswith('NORW'):
                try: 
                    record = BioMeta(unknown = EXCLUDE).load(x)
                    meta_data[record['central_sample_id']] = record
                except ValidationError as err:

                    logging.error(x['COG Sample ID'])
                    logging.error(err.messages)
                    errors[x['COG Sample ID']] = err.messages

    return meta_data, errors

def update_our_meta(new_data, client, sheet_name='SARCOV2-Metadata', force_update = True):
    messages = [] 
    old = None 
    if os.path.exists('old_samples'):
        old = [x.strip() for x in open('old_samples').readlines() ]
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)
    no_bio_meta_data = set(row_position) - set([x['central_sample_id'] for x in new_data.values()])
 #   no_bio_meta_data.pop('central_sample_id')
    if old:
        no_bio_meta_data = set(no_bio_meta_data) - set(old)
    if no_bio_meta_data : 
        logging.info('NO BIO META found for ' + ','.join(no_bio_meta_data))
        messages.append('No metadata in input sheet found for ' + ', '.join(no_bio_meta_data))
    cells_to_update = []
    duplicates = [item for item, count in collections.Counter(row_position).items() if count > 1]
    if duplicates:
        messages.append('Following records are duplicated in master sheet:' + ', '.join(duplicates))
        print('Following records are duplicated in master sheet: ' + ','.join(duplicates))
    missing_rows_names = list(set([x['central_sample_id'] for x in new_data.values()]) - set(row_position) )
    if missing_rows_names:
        print('Adding Missing ROWS\n' + '\n'.join(missing_rows_names))
        sheet.resize(len(row_position))
        values = [[x] for x in missing_rows_names]
        sheet.append_rows(values)
        all_values = sheet.get_all_records()
        column_position = sheet.row_values(1)
        row_position = sheet.col_values(1)
    no_sync = '' 
    for x in all_values:
        bio_metadata = new_data.get(x['central_sample_id'])
        if bio_metadata:
            for key, value in bio_metadata.items():
                if key in ['is_surveillance']:
                    continue
                # Handle date.
                if key == 'collection_date':
                    value =  value.strftime('%Y-%m-%d')                
                if x[key] == '':
                    # look up row position
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index(key)+1, value=value))
                elif x[key] != value:
                    no_sync += f"{x['central_sample_id']} : {key} != \"{value}\" It currently is \"{x[key]}\"\n"
                    print(f"{x['central_sample_id']} : {key} != \"{value}\" It currently is \"{x[key]}\"")
                    if force_update:
                        cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index(key)+1, value=value))
    if no_sync != '':
        messages.append(no_sync)
    if cells_to_update:
        print('Updating values')
        sheet.update_cells(cells_to_update)
    else:
        print('All values sync. Nothing to update')
    return messages

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
        if len(ids) > 1:
            dates = {k:date_lookup[k] for k in ids}
            min_id = sorted(dates.items(), key=lambda x: (x[1], x[0]))[0][0]
            for id in ids: 
                if id != min_id:
                    cells_to_update.append(gspread.models.Cell(row=row_position.index(id)+1, col=column_position.index('is_surveillance')+1, value='N'))    
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
