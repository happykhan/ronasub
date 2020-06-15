import xml.etree.ElementTree as ET
from ftplib import FTP
from csv import DictReader, excel , DictWriter, excel_tab
import pysam
import os 
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

def main(data_name, bam_dir, mapping_file=None, upload=False, suffix='sorted.bam'):
    # Fetch all samples in DIR
    sample_meta = {}
    mapping_files = {}
    if mapping_file:
        for x in csv.DictReader(open(mapping_file)):
            mapping_files[x['barcode']] = x['central_sample_id']

    for bam_file in os.listdir(bam_dir):
        if bam_file.endswith(suffix):
            sample_name = 'NORW-' + bam_file.split('_')[0]
            clean_file_name = data_name + '_' + bam_file
            if mapping_file:
                clean_file_name = data_name + '_' + mapping_files[bam_file.split('_')[1].split('.')[0]] + '.sorted.bam'
                sample_name = 'NORW-' + mapping_files[bam_file.split('_')[1].split('.')[0]]
            sample_meta[sample_name] = dict(filepath=os.path.join(bam_dir, bam_file), filename = clean_file_name, study=study)

    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)

    # Fetch metadata from master table. 
    sheet = client.open("CoronaHiT Supplementary Tables").worksheet("Sheet8")
    all_values = sheet.get_all_records()
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)    
    cells_to_update = [] 
    for x in all_values:
        sample = x['Sample name']
        # Some runs do not have all records
        if sample_meta.get(sample):
            sample_meta[sample]['biosample'] = x['BioSample accession']
            if len(x[data_name + ' checksum']) > 4:
                sample_meta[sample]['md5'] = x[ data_name + ' checksum']
            else:
                print('checksum for ' + sample)
                sample_meta[sample]['md5'] = subprocess.check_output(['md5sum', sample_meta[sample]['filepath']]).split()[0].decode('utf-8')
                cells_to_update.append(gspread.models.Cell(row=row_position.index(sample)+1, col=column_position.index( data_name + ' checksum')+1, value=sample_meta[sample]['md5']))
    if cells_to_update:
        print('Updating values')
        sheet.update_cells(cells_to_update)

    # Upload files to staging area.
    if upload:
        with FTP('webin.ebi.ac.uk')  as ftp:
            ftp.login(user='Webin-55756',passwd='RrU2!3!g^be')
            existing  = ftp.nlst()
            for sample, values in sample_meta.items():
                if values['filename'] not in existing:
                    print('uploading ' + values['filename'])
                    ftp.storbinary('STOR ' + values['filename'], open(values['filepath'], 'rb'))

    # Create experiment xml 
    all_exp = ET.Element('EXPERIMENT_SET')
    all_run = ET.Element('RUN_SET')
    for record, rec_values in sample_meta.items():
        if rec_values.get('md5'):
            tree = ET.parse('enasub/template_exp.xml')
            root = tree.getroot()
            experiment_alias = f'{record} {data_name}'
            root.set('alias', experiment_alias)
            root.find('TITLE').text = f'Large scale multiplexing of SARS-CoV-2 genomes using nanopore sequencing {record} - {data_name}'
            root.find('STUDY_REF').set('accession', rec_values['study'])
            root.find('DESIGN').find('SAMPLE_DESCRIPTOR').set('accession', rec_values['biosample'])
            all_exp.append(root)

            # Create Run xml
            tree = ET.parse('enasub/template_run.xml')
            root = tree.getroot()
            root.set('alias', f'{record} {data_name} reads')
            root_file = root.find('DATA_BLOCK/FILES/FILE')
            root.find('EXPERIMENT_REF').set('refname', experiment_alias)
            root_file.set('checksum', rec_values.get('md5'))
            root_file.set('filename', rec_values.get('filename'))
            all_run.append(root)

    with open(data_name + '_all_exp.xml', 'wb') as out: 
        out.write(ET.tostring(all_exp))
    with open(data_name + '_all_run.xml', 'wb') as out: 
        out.write(ET.tostring(all_run))


study = 'ERP122169'
bam_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20200522/ncovIllumina_sequenceAnalysis_readMapping'
data_name = 'Illumina'
bam_suffix = 'sorted.bam'
main(data_name, bam_dir, suffix=bam_suffix)

bam_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/cog13.coronahit/result.coronahit.20200527.cog13.f50.r40/articNcovNanopore_sequenceAnalysisMedaka_articMinIONMedaka'
data_name = 'CoronaHiT'
mapping_file = 'coronaont'
bam_suffix = 'sorted.bam'
main(data_name, bam_dir, mapping_file, suffix=bam_suffix, upload=True)

bam_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/cog13.coronahit/result.nanopore.20200602/articNcovNanopore_sequenceAnalysisMedaka_articMinIONMedaka'
data_name = 'ARTIC_ONT'
mapping_file = 'stdont'
bam_suffix = 'sorted.bam'
main(data_name, bam_dir, mapping_file, suffix=bam_suffix, upload=True)
