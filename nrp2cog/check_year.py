from marshmallow import EXCLUDE
from marshmallow import ValidationError
import re 
import matplotlib.pyplot as plt
from nrpschema import BioMeta
import collections
import gspread
import logging
import os 
from oauth2client.service_account import ServiceAccountCredentials
from nrp_util import get_google_session


scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)
sheet_name = 'SARCOV2-Metadata'
sheet = client.open(sheet_name).sheet1
row_position = sheet.col_values(1)
all_values = {k['central_sample_id']:k for k in sheet.get_all_records() } 
load = { }
nodate = [] 
print(f'Virus name\tAccession ID\tCollection date')
with open('no_date') as f: 
    for x in f.readlines(): 
        sample_id = x.split('/')[2]
        gis = x.split('\t')[0]
        acc = x.split('\t')[1].strip()
        if all_values.get(sample_id):
            date = all_values[sample_id].get('collection_date')
            if date:
                print(f'{sample_id}\t{acc}\t{date}')
            else:
                nodate.append(sample_id)
print(f'No date:\n' + '\n'.join(nodate))

