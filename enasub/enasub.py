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

study = 'ERP122169'
acc = 'temp/illumina_table'
# read_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/200415.coronahit/result.illumina.20200522/ncovIllumina_sequenceAnalysis_readMapping'
bam_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20200522/ncovIllumina_sequenceAnalysis_readMapping'
upload = True
convert = 'temp/new-convert'

# Import table mapping csv
# mapping = {}
# for x in DictReader(open(acc)): 
#     x['study'] = study
#     mapping[x['id']] = x

# # Deal with the names. -_- 
# illumina_mapping = {}
# for x in DictReader(open('temp/illumina_table')): 
#     x['study'] = study
#     illumina_mapping[x['id']] = x

# new_mapping = {}
# for x, y  in mapping.items():
#    real_key = 'NORW-' + y['cog']
#    new_mapping[real_key] = dict(id=real_key, acc=illumina_mapping[real_key]['acc'], barcode=y['barcode'], study=study)

#mapping = new_mapping
class ENAMeta(Schema):
    central_sample_id = fields.Str()
    tax_id = fields.Str(missing='2697049') 
    scientific_name = fields.Str(missing='Severe acute respiratory syndrome coronavirus 2')
    sample_title = fields.Str()
    sample_description = fields.Str()
    collection_date = fields.Str()
    country = fields.Str(missing='United Kingdom')
    adm2 = fields.Str()
    capture = fields.Str(missing='active surveillance in response to outbreak')
    host = fields.Str(missing='Human')
    source_age = fields.Integer()
    host_health_state = fields.Str(missing='not provided')
    source_sex = fields.Str(validate=validate.OneOf(['male', 'female']))
    host_scientific_name = fields.Str(missing='Homo sapiens')
    collector_name = fields.Str(missing="Justin O'Grady")
    collecting_org = fields.Str( missing="Quadram Institute Bioscience")
 
    @pre_load
    def clean_up(self, in_data, **kwargs):
        for k,v in dict(in_data).items():
            if v in [''] :
                in_data.pop(k)        
            elif isinstance(v, str):
                    in_data[k] = v.strip()
        if in_data.get('source_sex'):
            if in_data['source_sex'] == 'M':
                in_data['source_sex'] = 'male'
            if in_data['source_sex'] == 'F':
                in_data['source_sex'] = 'female'
        return in_data

    @post_dump
    def wash(self, in_data, **kwargs):
        in_data["geographic location (country and/or sea)"] = in_data["country"]
        in_data.pop('country')
        if in_data.get('adm2'):
            in_data["geographic location (region and locality)"] = in_data["adm2"]
            in_data.pop('adm2')        
        in_data["sample capture status"] = in_data["capture"]
        in_data.pop('capture')                
        in_data["host common name"] = in_data["host"]
        in_data.pop('host')     
        if in_data.get("source_age"):
            in_data["host age"] = str(in_data["source_age"])
            in_data.pop('source_age')
        if in_data.get("source_sex"):                            
            in_data["host sex"] = in_data["source_sex"]
            in_data.pop('source_sex')
        else:
            in_data["host sex"] = 'not provided'                                 
        in_data['host subject id'] = in_data['central_sample_id']
        in_data['isolate'] = in_data['central_sample_id']
        in_data['sample_alias'] = in_data['central_sample_id']
        in_data['sample_title'] = in_data['central_sample_id']
        in_data.pop('central_sample_id')
       # if in_data.get('collection_date'):
     #       in_data['collection date'] = datetime.strpfmt('%d/%m/%Y', in_data['collection_date']).strpfmt('%Y-%m-%d')
        in_data['collecting institution'] = in_data['collecting_org']
        in_data.pop('collecting_org')
        in_data['collector name'] = in_data['collector_name']
        in_data.pop("collector_name")
        in_data['host health state'] = in_data['host_health_state']
        in_data.pop("host_health_state")
        in_data['host scientific name'] = in_data['host_scientific_name']
        in_data.pop("host_scientific_name")        
        return in_data

# Fetch all samples in DIR
upload_samples = {}
for bam_file in os.listdir(bam_dir):
    sample_name = 'NORW-' + bam_file.split('_')[0]
    upload_samples[sample_name] = dict(bam_file=os.path.join(bam_dir, bam_file))


scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)

# Fetch metadata from master table. 
sheet = client.open("SARCOV2-Metadata").sheet1
all_values = sheet.get_all_records()
sample_meta = {}
for x in all_values:
    if upload_samples.get(x['central_sample_id']):
        sample_meta[x['central_sample_id']] = ENAMeta(unknown = EXCLUDE).load(x)

# Create sample xml
all_samples = ET.Element('SAMPLE_SET')
fields_name = dict(sample_alias=True, scientific_name=True)
for k,v in sample_meta.items():
    tree = ET.parse('enasub/template_sample.xml')
    root = tree.getroot()
    our_dat = ENAMeta(unknown = EXCLUDE).dump(v)
    our_dat.pop('scientific_name')
    root.set('alias', our_dat.pop('sample_alias'))    
    root.find('TITLE').text = our_dat['isolate']    
    attr_list = root.find('SAMPLE_ATTRIBUTES')
    for key, value in our_dat.items():
        fields_name[key] = True
        new_attr = ET.SubElement(attr_list, 'SAMPLE_ATTRIBUTE')
        new_tag = ET.SubElement(new_attr, 'TAG')
        new_tag.text = key
        new_value = ET.SubElement(new_attr, 'VALUE')
        new_value.text = value
    all_samples.append(root)

with open('all_sample.xml', 'wb') as out: 
     out.write(ET.tostring(all_samples))

field_list = sorted(list(fields_name.keys()))
out = DictWriter(open('sample.tsv', 'w'), fieldnames=field_list, dialect=excel_tab)
out.writeheader()
for k, v in sample_meta.items():
    out.writerow(ENAMeta(unknown = EXCLUDE).dump(v))
# add the stupid headers

with open('sample.tsv', 'r') as f :
    out = open('clean_sample.tsv', 'w')
    out.write('#checklist_accession\tERC000033\n')
    out.write('#unique_name_prefix\n')
    count = 0
    for x in f.readlines():
        if count == 1:
            out.write('#template\n')
            out.write('#units\n')
        out.write(x)
        count += 1 



# Create run XML. 


# Update accession table. 
sheet = client.open("CoronaHiT Supplementary Tables").worksheet("Sheet8")


# Locate bam files 
# has_files = [] 
# for sample_dir in os.listdir(bam_dir):
#     sample_path = os.path.join(bam_dir, sample_dir)
#     if os.path.isdir(sample_path):
#         for x in os.listdir(sample_path):
#             if x.endswith('.bam'):
#                 bam_file_path = os.path.join(sample_path, x)
#                 if mapping.get(sample_dir):            
#                     mapping[sample_dir]['filename'] = x
#                     mapping[sample_dir]['filepath'] = bam_file_path
#                 else:
#                     ugh = [x for x in mapping.values() if sample_dir.split('_')[1] == x['barcode']]
#                     if ugh:
#                         real_id = [x for x in mapping.values() if sample_dir.split('_')[1] == x['barcode']][0]['id']
#                         mapping[real_id]['filename'] = x
#                         mapping[real_id]['filepath'] = bam_file_path                        




# # Create md5 hashes
# for sample in mapping:
#     if not mapping[sample].get('md5') or True:
#         mapping[sample]['md5'] = subprocess.check_output(['md5sum', mapping[sample]['filepath']]).split()[0].decode('utf-8')
        
#         #mapping[sample]['md5'] = hashlib.md5(open(mapping[sample]['filepath'],'rb').read()).hexdigest()

# acc_table = DictWriter(open(acc, 'w'), fieldnames=['id', 'acc', 'md5'], extrasaction='ignore')
# acc_table.writeheader()
# acc_table.writerows(mapping.values())
                
# # Upload files to staging area.
# if upload:
#     with FTP('webin.ebi.ac.uk')  as ftp:
#         ftp.login(user='Webin-55756',passwd='RrU2!3!g^be')
#         existing  = ftp.nlst()
#         for sample, values in mapping.items():
#             if values['filename'] not in existing:
#                 ftp.storbinary('STOR ' + values['filename'], open(values['filepath'], 'rb'))


# # Create experiment xml 
# all_exp = ET.Element('EXPERIMENT_SET')
# all_run = ET.Element('RUN_SET')
# for record, rec_values in mapping.items():
#     if rec_values.get('md5'):
#         tree = ET.parse('enasub/template_exp.xml')
#         root = tree.getroot()
#         experiment_alias = f'{rec_values["id"]} ONT'
#         root.set('alias', experiment_alias)
#         root.find('TITLE').text = f'Large scale multiplexing of SARS-CoV-2 genomes using nanopore sequencing {rec_values["id"]} - ONT'
#         root.find('STUDY_REF').set('accession', rec_values['study'])
#         all_exp.append(root)

#         # Create Run xml
#         tree = ET.parse('enasub/template_run.xml')
#         root = tree.getroot()
#         root.set('alias', f'{rec_values["id"]} ONT reads')
#         root_file = root.find('DATA_BLOCK/FILES/FILE')
#         root.find('EXPERIMENT_REF').set('refname', experiment_alias)
#         root_file.set('checksum', rec_values.get('md5'))
#         root_file.set('filename', rec_values.get('filename'))
#         all_run.append(root)

# with open('all_exp.xml', 'wb') as out: 
#     out.write(ET.tostring(all_exp))
# with open('all_run.xml', 'wb') as out: 
#     out.write(ET.tostring(all_run))