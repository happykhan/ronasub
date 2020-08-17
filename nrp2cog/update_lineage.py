from marshmallow import EXCLUDE
from nrpschema import lineageMeta
import gspread
import logging 
import collections

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
    logging.info('No lineage META found for ' + '\n'.join(no_lineage_meta_data))
    cells_to_update = []
    logging.info('Following records are duplicated in master sheet:' + ','.join([item for item, count in collections.Counter(row_position).items() if count > 1]))
    logging.info('PLEASE ADD THESE ROWS\n' + '\n'.join(list(set(new_data.keys()) - set(row_position) )))
    for x in all_values:
        lineage_metadata = new_data.get(x['central_sample_id'])
        if lineage_metadata:
            for key, value in lineage_metadata.items():
                cells_to_update.append(gspread.models.Cell(row=row_position.index(x['central_sample_id'])+1, col=column_position.index(key)+1, value=value))
        else:
            logging.info('NO LINEAGE METADATA  FOR ' + x['central_sample_id'])
    if cells_to_update:
        logging.info('Updating values')
        sheet.update_cells(cells_to_update)
    else:
        logging.info('All values sync. Nothing to update')
