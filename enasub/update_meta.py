from Bio import SeqIO
from statistics import mean
import gzip 
import subprocess
import hashlib
import gspread 
from oauth2client.service_account import ServiceAccountCredentials
from marshmallow import Schema, fields, EXCLUDE, pre_load, validate, post_dump
import datetime
import csv
import gspread

acc = [x for x in csv.DictReader(open('temp/acc'), dialect=csv.excel_tab)]

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)
import re 
all_ass = []
for x in csv.DictReader(open('temp/ass'), dialect=csv.excel_tab):
    if x['fileName'].endswith('.fasta.gz'):
        platform = x['fileName'].split('_')[0]
        if x['fileName'].startswith('CoronaHiT-48'):
            id = x['fileName'].split('_')[1].split('.')[0]  
        elif x['fileName'].startswith('CoronaHiT_'):
            platform = 'CoronaHiT-95'
            id = x['fileName'].split('_')[1].split('.')[0]  
        elif x['fileName'].startswith('Illumina'):
            id = x['fileName'].split('_')[1].split('.')[0]  
            if not id.startswith('NORW'):
                id = 'NORW_' + id 
        elif x['fileName'].startswith('ARTIC_ONT'):
            platform = 'ARTIC_ONT'
            id = x['fileName'].split('_')[2].split('.')[0]              
        accxx = x['id']
        all_ass.append(dict(platform = platform, acc= accxx, id=id))
    
    

# Fetch metadata from master table. 
sheet = client.open("CoronaHiT Supplementary Tables").worksheet("Sheet8")
all_values = sheet.get_all_records()
column_position = sheet.row_values(1)
row_position = sheet.col_values(1)    
cells_to_update = [] 
for x in all_values:
    sample = x['Sample name']
    # Some runs do not have all records
    for zz in all_ass:
        if zz['id'] == sample:
            data_name = zz['platform'] + ' consensus'
            cells_to_update.append(gspread.models.Cell(row=row_position.index(sample)+1, col=column_position.index( data_name )+1, value=zz['acc']))

    # for acc_rec in acc: 
    #     if acc_rec['TYPE'] == 'RUN':
    #         if acc_rec['ALIAS'].split()[0] == sample:
    #             data_name = acc_rec['ALIAS'].split()[1]
    #             cells_to_update.append(gspread.models.Cell(row=row_position.index(sample)+1, col=column_position.index( data_name )+1, value=acc_rec['ACCESSION']))
if cells_to_update:
    print('Updating values')
    sheet.update_cells(cells_to_update)


