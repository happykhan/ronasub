"""
Mega sub runs through the datadir and detects all plates and runs and determines if they're worth uploading. 


"""
import os 
import re

datadir = "/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq"
read_dirs = [ '/home/ubuntu/transfer/incoming/QIB_Sequencing/Nextseq_1_runs', "/home/ubuntu/transfer/incoming/QIB_Sequencing/Nextseq_2_runs"]

datadirs = {}

for x in os.listdir(datadir): 
    dir_name_match = re.match('result\.(\w+)\.(\d+)$|result\.(\w+)\.(\d+)-(.+)$', x)
    if dir_name_match: 
        if dir_name_match.group(1):
            platform = dir_name_match.group(1)
            run_name = dir_name_match.group(2)
            context = None
        else:
            platform = dir_name_match.group(3)
            run_name = dir_name_match.group(4)
            context = dir_name_match.group(5)            
        if not platform or not run_name:
            print('Error with parsing folder ' + x)
        datadirs[x] = dict(platform=platform, run_name = run_name)
        if context:
            datadirs[x]['context'] = context
all_samples = {}
# Get seq_run_id  Too in next_1_run or next_2_run 
for seq_dir in read_dirs: 
    for read_dir in os.listdir(seq_dir):
        for key, values in datadirs.items(): 
            if read_dir.startswith(values['run_name'][2:]):
                os.path.join(seq_dir, read_dir)
                if values.get('read_dirs'):
                    values['read_dirs'].append(read_dir)
                    print('Ambigious date ' + read_dir) 
                else:
                    values['read_dirs'] = [read_dir]
