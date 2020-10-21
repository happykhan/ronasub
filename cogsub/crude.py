
path = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/result.illumina.20200924/NORW-20200924.qc.csv'
import csv 
with open(path) as f:
    for r in csv.DictReader(f, dialect=csv.excel):
        if r['sample_name'].split('_')[0].endswith('crude-prep'):
            print(r['sample_name'])
