import csv 
import logging
import os 

def  get_civet_metadata(rundir): 
    meta_path = os.path.join(rundir, 'civet', 'combined_metadata.csv')
    if os.path.exists(meta_path):
        logging.info(f'Reading {meta_path}')
        with open(meta_path) as f: 
            data = {}
            for x in csv.DictReader(f, dialect=csv.excel):
                new_dict = {}
                new_dict['civet_lineage'] = x['lineage']
                new_dict['civet_lineage_support'] = x['lineage_support']
                new_dict['civet_uk_lineage'] = x['uk_lineage']
                new_dict['civet_phylotype'] = x['phylotype']
                if x.get('closest_distance'):
                    new_dict['civet_closest_distance'] = x.get('closest_distance')
                new_dict['civet_acc_lineage'] = x['acc_lineage']
                new_dict['civet_del_lineage'] = x['del_lineage']
                data[x['central_sample_id']] = new_dict

        return data
    else:
        logging.info(f'NO combined metadata sheet found in {rundir}')
        return None

from gspread.models import Cell
            
def update_civet_meta(new_dict, client, sheet_name='SARCOV2-Metadata'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)
    cells_to_update = []
    for x in all_values:
        civet_metadata = new_dict.get(x['central_sample_id'])
        if civet_metadata:
            for k, v in civet_metadata.items():
                if k in column_position:
                    cells_to_update.append(Cell( 
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index(k)+1,
                        value=v))                   
                                                   
    if cells_to_update:
        logging.info('Updating values')
        sheet.update_cells(cells_to_update)
    else:
        logging.info('All values sync. Nothing to update')