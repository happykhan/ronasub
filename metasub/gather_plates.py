"""
gather_plates reads covid data dir and locates all sequenced samples. 

Requires login for google sheets

### CHANGE LOG ### 
2021-04-19 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build - split from cogsub 
"""


"""
it should 
* go through the all the sequencing runs Nextseq_1 and nextseq_2 dirs 
* find the samplesheet (not every run has a sample sheet) 
*  Pull out all the covid samples and group them according to "called" and figure out which plate they are. the code i pasted above might help
* then update the google sheet - coguksubmission status . add new records if not there, check library name, run name are as expected. """




import os
import os.path

import logging 

def gather():
    logging.info('Not implemented')
    print('Hello World')

    sampleName2Project=dict()
    sampleName2Called=dict()
    sampleName2RunName=dict()

    for next_seq_directory in ('Nextseq_1_runs','Nextseq_2_runs'):
        directories = os.listdir('transfer/incoming/QIB_Sequencing/' + next_seq_directory)
        for directory in directories:
            if os.path.isdir('transfer/incoming/QIB_Sequencing/' + next_seq_directory + '/' + directory):
                sample_sheet_file = 'transfer/incoming/QIB_Sequencing/' + next_seq_directory + '/' + directory + '/SampleSheet.csv'
                if os.path.isfile(sample_sheet_file):
                    print(sample_sheet_file)
                    try:
                        with open(sample_sheet_file) as f:
                            lines = f.readlines()
                            for line in lines:
                                fields = line.rstrip().split(',')
                                sampleName = fields[0]
                                if len(fields)>7:
                                    sampleName2Project[sampleName] = fields[len(fields)-1]
                                    sampleName2Called[sampleName] = fields[2]
                                    sampleName2RunName[sampleName] = str(directory)
                    except:
                        pass

    for sampleName in sampleName2RunName.keys():
        print(sampleName + ',' + sampleName2RunName[sampleName] + ',' + sampleName2Called[sampleName] + ',' + sampleName2Project[sampleName])

gather()
