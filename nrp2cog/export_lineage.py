import gspread
import logging 

def update_pub_info(client, sheet_name='SARCOV2-Metadata', out_sheet_name='Sample-lineages'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    load = { }
    for x in all_values:
        if len(x['uk_lineage']) > 2 : 
            date = ''
            if len(x['collection_date']) > 3 :
                date = x['collection_date']
            else:
                date = x['received_date']
            if not x['central_sample_id'].endswith('duplicate'):
                load['England/' + x['central_sample_id'] + '/2020'] = { "uk_lineage": x['uk_lineage'], "biosample_source_id": x["biosample_source_id"], "date": date, "county" : x['adm2'], "age" : x['source_age'], "sex" : x['source_sex']}
    sheet = client.open(out_sheet_name).sheet1
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)      
    all_values = sheet.get_all_records()
    cells_to_update = [] 

    logging.info('ADD ROWS TO REPORT\n' +  '\n'.join(list(set(load.keys() - set(row_position)))))
    for x in all_values:
        if load.get(x['sample_id']):
            for id, value in load.get(x['sample_id']).items(): 
                cells_to_update.append(gspread.models.Cell(row=row_position.index(x['sample_id'])+1, col=column_position.index(id)+1, value=value))    
    if cells_to_update:
        logging.info('Updating values')
        sheet.update_cells(cells_to_update)
            
