import csv
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import logging
import csv 
import os 
import pandas as pd
import matplotlib.pyplot as plt

def post_code_lookup(our_list, file_name='post_code.txt', remove_zero=True):
    data = {}
    lat_long = [x for x in csv.DictReader(open("cogsub/post_code.txt"))] 
    for x in lat_long:
        data[x['postcode']] = x
        data[x['postcode']].pop('id')
        data[x['postcode']]['count'] = 0
    for x in our_list:
        data[x]['count'] += 1
    if remove_zero:
        out = [x for x in data.values() if x['count'] > 0]
    else:
        out = [x for x in data.values() ]
    return out


def post_code_list(client, sheet_name='SARCOV2-Metadata'):
    sheet = client.open(sheet_name).sheet1
    all_values = sheet.get_all_records()
    values = []
    for x in all_values:
        if x['adm2_private'] != '':
            values.append(x['adm2_private'])
    return values

  


if not os.path.exists('clean_pc_table.txt'):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('cogsub/credentials.json', scope)
    client = gspread.authorize(creds)
    our_list = post_code_list(client)
    clean_table = post_code_lookup(our_list)
    with open('clean_pc_table.txt', 'w') as table:
        out_table = csv.DictWriter(table, fieldnames=['postcode', 'latitude', 'longitude', 'count'])
        out_table.writeheader()
        out_table.writerows(clean_table)


import cartopy.crs as ccrs
import matplotlib.pyplot as plt
fig = plt.figure()
pc_map = pd.read_csv('clean_pc_table.txt')

cen_long =  sum(pc_map['longitude']) / len(pc_map['longitude'])
cen_lat =  sum(pc_map['latitude']) / len(pc_map['latitude'])

ax = fig.add_subplot(1, 1, 1,projection=ccrs.PlateCarree())

                     # projection=ccrs.AzimuthalEquidistant(central_longitude=cen_long, central_latitude=cen_lat ))

# ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent([min(pc_map['longitude']) - 0.1, max(pc_map['longitude']) + 0.1, min(pc_map['latitude']) - 0.1, max(pc_map['latitude']) + 0.1])
ax.scatter(pc_map['longitude'], pc_map['latitude'], c=pc_map['count'])
ax.coastlines()
plt.savefig('test.png')

 