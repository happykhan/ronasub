import csv 
import os
# look up mapping file. 
map = {}
for x in csv.DictReader(open('our_list', 'r')):
    map[x["cog"].split('/')[2]] = x['gis']
# find samples from bad run 
bad_dir = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/200415.coronahit/result.illumina.20200418/ncovIllumina_sequenceAnalysis_makeConsensus/'
bad_files_in_gis = {}
for x in os.listdir(bad_dir): 
    sample_name = 'NORW-'  + x[0:5]
    if map.get(sample_name):
        bad_files_in_gis[sample_name] = dict(gis=map.get(sample_name), sample=sample_name) 
# find replacement sequences.  (latest)
dirs = ["result.illumina.20200429", "result.illumina.20200513", "result.illumina.20200516", "result.illumina.20200519", "result.illumina.20200522", "result.illumina.20200602"]
for dir in dirs: 
    path = '/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/' + dir + '/ncovIllumina_sequenceAnalysis_makeConsensus/'
    for ass in os.listdir(path):
        sample_name = 'NORW-'  + ass[0:5]
        if bad_files_in_gis.get(sample_name):
            file_path = os.path.join(path, ass)
            if bad_files_in_gis[sample_name].get('filepath'):
                print('WARN already done' + sample_name)
            bad_files_in_gis[sample_name]['filepath'] = file_path
# Create a tarball for the woggles
if not os.path.exists('/home/ubuntu/gis'):
    os.mkdir('/home/ubuntu/gis')
import shutil
print('ID\tSAMPLENAME\tFILENAME')
for x,y  in bad_files_in_gis.items():
    if y.get('filepath'):    
        filenames = y['gis'] + '.fasta'
        print(f'{y["gis"]}\t{y["sample"]}\t{filenames}')
        shutil.copyfile(y.get('filepath'), '/home/ubuntu/gis/' + filenames)




