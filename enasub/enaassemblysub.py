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

def main(data_name, bam_dir, upload=False, suffix='.consensus.fasta'):
    # create manifest files. 
    if not os.path.exists('mani'):
        os.mkdir('mani')
    all_mani = {}
    mapping_files = {}
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)

    if data_name != 'Illumina':
        sheet = client.open("CoronaHiT Supplementary Tables").worksheet("cog barcode")
        all_values = sheet.get_all_records()
        for x in all_values:
            sample = x['central_sample_id']
            jj = ''
            if data_name == 'CoronaHiT-48':
                jj = 'coronaHiT_48'

            if len(x[jj]) > 1 :
                mapping_files[x[jj]] = sample

    # Fetch all samples in DIR
    for bam_file in os.listdir(bam_dir):
        if bam_file.endswith(suffix) and not bam_file.lower().startswith('blank'):
            sample_name = 'NORW-' + bam_file.split('_')[0]
            clean_file_name = data_name +'_' + sample_name
            if mapping_files:
                clean_file_name = data_name + '_NORW-' + mapping_files[bam_file.split('_')[1].split('.')[0]] + suffix
                sample_name = 'NORW-' + mapping_files[bam_file.split('_')[1].split('.')[0]]
            all_mani[sample_name] = dict(filepath=os.path.join(bam_dir, bam_file), filename = clean_file_name, STUDY=study)

    # Fetch metadata from master table. 
    sheet = client.open("CoronaHiT Supplementary Tables").worksheet("Sheet8")
    all_values = sheet.get_all_records()
    for x in all_values:
        sample = x['Sample name']
        # Some runs do not have all records
        if all_mani.get(sample):
            all_mani[sample]['SAMPLE'] = x['BioSample accession']
            all_mani[sample]['RUN_REF'] = x[ data_name]
    #Get Coverage 
    # sheet = client.open("CoronaHiT Supplementary Tables").worksheet("Sheet11")
    # all_values = sheet.get_all_records()
    # for x in all_values:
    #     sample = x['Sample name']
    #     if all_mani.get(sample):
    #         all_mani[sample]['COVERAGE'] = float(x[data_name + ' Coverage'][:-1])

    # Run webin tool for each file 
    if not os.path.exists('mani_out'):
        os.mkdir('mani_out')
    doscript = []
    for x, y in all_mani.items():
        mani_file = os.path.join('mani', y['filename'] + '.mani')
        gzip_reads = 'mani/' + y["filename"] + '.fasta.gz'
        gzip_chromo = 'mani/' + y["filename"] + '.chrom.gz'
        with open(mani_file, 'w') as f:
            f.write(f'STUDY\t{y["STUDY"]}\n')
            f.write(f'SAMPLE\t{y["SAMPLE"]}\n')
            f.write(f'RUN_REF\t{y["RUN_REF"]}\n')
            f.write(f'FASTA\t{os.path.basename(gzip_reads)}\n')
            f.write(f'NAME\t{y["filename"].split(".")[0]}\n')
            f.write(f'ASSEMBLY_TYPE\tCOVID-19 outbreak\n')
            if data_name == 'Illumina':
                f.write(f'PROGRAM\tARTIC-ivar\n')
                f.write(f'PLATFORM\tIllumina\n')                   
            else:
                f.write(f'PROGRAM\tARTIC-Medaka\n')
                f.write(f'PLATFORM\tOxford Nanopore\n')          
            f.write(f'COVERAGE\t{y.get("COVERAGE", "1000")}\n')
            f.write(f'CHROMOSOME_LIST\t{os.path.basename(gzip_chromo)}\n')
        gzip_out = gzip.open(gzip_reads, 'wb')
     #       clean_fasta = 
        gzip_out.write(open(y["filepath"]).read().encode('utf-8'))
        gzip_out.close()
        gzip_out = gzip.open(gzip_chromo, 'wb')
        fasta_header = open(y["filepath"]).readlines()[0][1:].split()[0].strip()
        chromo_string = f'{fasta_header}\t1\tChromosome\n'
        gzip_out.write(chromo_string.encode('utf-8'))
        gzip_out.close()      
        if os.path.basename(mani_file).startswith('Illumina_NORW-EAED5') or  os.path.basename(mani_file).startswith('Illumina_NORW-EB51B') or  os.path.basename(mani_file).startswith('Illumina_NORW-EAE7B') or  os.path.basename(mani_file).startswith('Illumina_NORW-EB3C6') or  os.path.basename(mani_file).startswith('Illumina_NORW-EA224')  :

            doscript.append(f'java -jar webin-cli-3.0.0.jar -context genome -centerName QIB -manifest {mani_file} -inputDir mani -outputDir mani_out  -submit  -userName Webin-55756 -passwordFile pass')
    return doscript


study = 'ERP122169'
list_of_cli = [] 

# bam_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/cog13.coronahit/result.coronahit.20200610.cog13.f50.r40/articNcovNanopore_sequenceAnalysisMedaka_articMinIONMedaka/'
# data_name = 'CoronaHiT-48'
# mapping_file = 'coronaont'
# list_of_cli += main(data_name, bam_dir, mapping_file)

# bam_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/cog13.coronahit/result.coronahit.20200527.cog13.f50.r40/articNcovNanopore_sequenceAnalysisMedaka_articMinIONMedaka'
# data_name = 'CoronaHiT'
# mapping_file = 'coronaont'
# list_of_cli += main(data_name, bam_dir, mapping_file)

# bam_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/cog13.coronahit/result.nanopore.20200602/articNcovNanopore_sequenceAnalysisMedaka_articMinIONMedaka'
# data_name = 'ARTIC_ONT'
# mapping_file = 'stdont'
# list_of_cli += main(data_name, bam_dir, mapping_file)

bam_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20200522/ncovIllumina_sequenceAnalysis_makeConsensus/'
data_name = 'Illumina'
list_of_cli += main(data_name, bam_dir, suffix = '.primertrimmed.consensus.fa')

z = open('do.sh', 'w')
z.write('\n'.join(list_of_cli))