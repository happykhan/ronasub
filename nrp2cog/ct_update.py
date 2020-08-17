
from nrpschema import CtMeta
import collections
from gspread.models import Cell
from marshmallow import EXCLUDE
import logging


def get_ct_metadata(client, sheet_name='cov-ct'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    cts = {}
    for x in all_values:
        if x['central_sample_id'].startswith('NORW'):
            record = CtMeta(unknown=EXCLUDE).load(x)
            cts[x['central_sample_id']] = record
    return cts


def update_ct_meta(new_data, client, sheet_name='SARCOV2-Metadata'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)
    no_ct_meta_data = set(row_position) - set(new_data.keys())
    logging.info('No CT META found for ' + ','.join(no_ct_meta_data))
    cells_to_update = []
    duplicate_sheet = ','.join(
        [item for item, count in collections.Counter(row_position).items() 
            if count > 1])
    logging.info(f'Following records are duplicated in master sheet: \
         {duplicate_sheet}')
    logging.info('PLEASE ADD THESE ROWS\n' + '\n'.join(list(set(new_data.keys()) - set(row_position))))
    for x in all_values:
        ct_metadata = new_data.get(x['central_sample_id'])
        if ct_metadata:
            if ct_metadata.get('ct_1_ct_value'):
                if[x['ct_1_ct_value']] != ct_metadata['ct_1_ct_value']:
                    cells_to_update.append(Cell( 
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_1_ct_value')+1,
                        value=ct_metadata['ct_1_ct_value']))
                if ct_metadata.get('ct_1_test_platform') == "AusDiagnostics":
                    cells_to_update.append(Cell(
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_1_test_platform')+1,
                        value='AUSDIAGNOSTICS'))
                    cells_to_update.append(Cell(
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_1_test_kit')+1,
                        value='AUSDIAGNOSTICS'))
                    cells_to_update.append(Cell(
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_1_test_target')+1,
                        value='ORF1AB'))
                if ct_metadata.get('ct_1_test_platform') == "Roche Cobas":
                    cells_to_update.append(Cell(
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_1_test_platform')+1,
                        value='ROCHE_COBAS'))
                    cells_to_update.append(Cell(
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_1_test_kit')+1,
                        value='ROCHE'))
                    cells_to_update.append(Cell(
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_1_test_target')+1,
                        value='RDRP'))                    

            if ct_metadata.get('ct_2_ct_value'):
                if[x['ct_2_ct_value']] != ct_metadata['ct_2_ct_value']:
                    cells_to_update.append(Cell(
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_2_ct_value')+1,
                        value=ct_metadata['ct_2_ct_value']))
                if ct_metadata.get('ct_2_test_platform') == "AusDiagnostics":
                    cells_to_update.append(Cell(
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_2_test_platform')+1,
                        value='AUSDIAGNOSTICS'))
                    cells_to_update.append(Cell(
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_2_test_kit')+1,
                        value='AUSDIAGNOSTICS'))
                    cells_to_update.append(Cell(
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_2_test_target')+1,
                        value='ORF8'))                    
                if ct_metadata.get('ct_2_test_platform') == "Roche Cobas":
                    cells_to_update.append(Cell(
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_2_test_platform')+1,
                        value='ROCHE_COBAS'))
                    cells_to_update.append(Cell(
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_2_test_kit')+1,
                        value='ROCHE'))
                    cells_to_update.append(Cell(
                        row=row_position.index(x['central_sample_id'])+1,
                        col=column_position.index('ct_2_test_target')+1,
                        value='E'))                                        
        else:
            logging.info('NO CT METADATA  FOR ' + x['central_sample_id'])
    if cells_to_update:
        logging.info('Updating values')
        sheet.update_cells(cells_to_update)
    else:
        logging.info('All values sync. Nothing to update')