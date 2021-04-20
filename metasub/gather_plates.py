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
    sampleName2Plate=dict()
    sampleName2RunName=dict()

    for next_seq_directory in ('Nextseq_1_runs','Nextseq_2_runs'):
        directories = os.listdir('transfer/incoming/QIB_Sequencing/' + next_seq_directory)
        for directory in directories:
            if os.path.isdir('transfer/incoming/QIB_Sequencing/' + next_seq_directory + '/' + directory):
                sample_sheet_file = 'transfer/incoming/QIB_Sequencing/' + next_seq_directory + '/' + directory + '/SampleSheet.csv'
                if os.path.isfile(sample_sheet_file):
                    try:
                        called2plate=dict()

                        with open(sample_sheet_file) as f:
                            lines = f.readlines()
                            for line in lines:
                                fields = line.rstrip().split(',')
                                sampleName = fields[0]
                                if (sampleName[-3:]=='_PC' or sampleName[-3:]=='_NC') and not sampleName[:4]=='ARCH':
                                    called2plate[fields[2]] = sampleName[:sampleName.index('_')]
                                elif sampleName[:5]=='BLANK': # Starts with blank
                                    if '_' in sampleName: called2plate[fields[2]] = sampleName[sampleName.index('_')+1:]
                                    else:
                                        plate_name = sampleName[5:]
                                        if plate_name[:1]=='c' or plate_name[:1]=='R': plate_name = plate_name[1:]
                                        called2plate[fields[2]] = plate_name
                                elif sampleName[-5:]=='BLANK': # Ends with blank
                                    if '_' in sampleName: called2plate[fields[2]] = sampleName[:sampleName.index('_')]
                                    else: called2plate[fields[2]] = sampleName[:-5]
                            
                            for line in lines:
                                fields = line.rstrip().split(',')
                                sampleName = fields[0]
                                if len(fields)>7:
                                    sampleName2Project[sampleName] = fields[len(fields)-1]
                                    sampleName2Called[sampleName] = fields[2]

                                    # Firstly try to infer the plate name from the sample name
                                    if sampleName[:8]=='ARCH-000' or sampleName[:8]=='ARCH_000': sampleName2Plate[sampleName] = sampleName[8:11]
                                    elif sampleName[:5]=='IPSOS':
                                        plate_name = sampleName[sampleName.index('_P')+2:]

                                        rows=('A','B','C','D','E','F','G','H')
                                        for row in rows:
                                            if row in plate_name: plate_name = plate_name[:plate_name.index(row)]

                                        if plate_name[-1:]=='_': plate_name = plate_name[:-1]
                                        
                                        sampleName2Plate[sampleName] = plate_name
                                    elif sampleName[:8]=='Re_array' or sampleName[:8]=='Re-array':
                                        plate_name = sampleName[sampleName.index('_pl')+3:]

                                        rows=('A','B','C','D','E','F','G','H')
                                        for row in rows:
                                            if row in plate_name: plate_name = plate_name[:plate_name.index(row)]

                                        if plate_name[-1:]=='_': plate_name = plate_name[:-1]
                                        
                                        sampleName2Plate[sampleName] = plate_name
                                    elif fields[2] in called2plate.keys(): sampleName2Plate[sampleName] = called2plate[fields[2]]
                                    else: sampleName2Plate[sampleName] = "Unknown"
                                    
                                    sampleName2RunName[sampleName] = str(directory)
                    except:
                        pass



# Now iterate through the covid results and output everything...use the metrics.csv
    directories = os.listdir('transfer/incoming/QIB_Sequencing/Covid-19_Seq')
    for directory in directories:
        if os.path.isdir('transfer/incoming/QIB_Sequencing/Covid-19_Seq/' + directory):
            directory_name = str(directory)
            if directory_name[:7]=='result.':
                files = os.listdir('transfer/incoming/QIB_Sequencing/Covid-19_Seq/' + directory)
                for file in files:
                    filename = str(file)
                    if 'metrics' in filename:
                        with open('transfer/incoming/QIB_Sequencing/Covid-19_Seq/' + directory + '/' + filename) as f:
                            lines = f.readlines()
                            first_line=True
                            for line in lines:
                                if first_line==True: first_line=False
                                else:
                                    fields = line.rstrip().split(',')
                                    central_sample = fields[0]
                                    if central_sample in sampleName2RunName.keys():
                                        print(central_sample + ',' + sampleName2RunName[central_sample] + ',' + sampleName2Plate[central_sample] + ',' + sampleName2Called[central_sample] + ',' + sampleName2Project[central_sample] + ',' + fields[12] + ',' + fields[13])
                                    else:
                                        print(central_sample + ',Not found')
                                    
gather()
