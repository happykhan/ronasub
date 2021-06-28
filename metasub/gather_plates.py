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
import time
import argparse
import sys
import meta
import csv
import gspread
import json

from oauth2client.service_account import ServiceAccountCredentials

epi = "Licence: " + meta.__licence__ +  " by " +meta.__author__ + " <" +meta.__author_email__ + ">"
logging.basicConfig()
log = logging.getLogger()

def get_google_session(credentials='credentials.json'):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials, scope)
    client = gspread.authorize(creds)
    return client

def gather(args):
    datadir = args.datadir
    nextseqdirs = args.nextseqdirs.split(';')
    resultsdirs = list()
    resultsdirs.append(args.resultdir)
    
    csvfile = open('gather_plates.csv','w+')
    csvfile.write('central_sample_id,library_name,run_name,sequencing_date,upload_date,plate_failed,pag_count,pags,metadata_sync,is_submitted_to_cog,partial_submission,library_type,plate,consensus_constructed,basic_qc,high_quality_qc\n')

    sampleName2Project=dict()
    sampleName2Called=dict()
    sampleName2Plate=dict()
    sampleName2RunName=dict()
    sampleName2SequencingDate=dict()

    for next_seq_directory in nextseqdirs:
        directories = os.listdir(datadir +'/' + next_seq_directory)
        for directory in directories:
            library_directory = datadir + '/' + next_seq_directory + '/' + directory
            if os.path.isdir(library_directory):
                files = os.listdir(library_directory)
                for file in files:
                    filename=str(file)
                    if 'SampleSheet' in filename and not filename[0]=='.' and os.path.isfile(library_directory + '/' + filename):
                        sample_sheet_file = library_directory + '/' + filename
                        called2plate=dict()

                        with open(sample_sheet_file, encoding="latin-1") as f:
                            lines = f.readlines()
                            if len(lines)<=2:
                                print('Cannot find data in file [' + sample_sheet_file + ']')
                            else:
                                date_line = lines[2].rstrip()
                                sequencing_date='Unknown'
                                if date_line[:4]=='Date':
                                    sequencing_date = date_line[date_line.index(',')+1:]
                                    sequencing_date = sequencing_date[6:10] + '-' + sequencing_date[3:5] + '-' + sequencing_date[0:2]
                                    
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
                                    key = fields[0] + directory[:6] # central_sampleid+date
                                    
                                    if len(fields)>7 and sampleName != 'Sample_ID':
                                        sampleName2SequencingDate[key] = sequencing_date
                                        sampleName2RunName[key] = str(directory)
                                        
                                        project = fields[len(fields)-1]
                                        if '-' in project: project = project[:project.index('-')]
                                        if '_' in project: project = project[:project.index('_')]
                                        sampleName2Project[key] = project
                                        
                                        sampleName2Called[key] = fields[2]

                                        # Firstly try to infer the plate name from the sample name
                                        if sampleName[:8]=='ARCH-000' or sampleName[:8]=='ARCH_000': sampleName2Plate[key] = sampleName[8:11]
                                        elif sampleName[:5]=='IPSOS':
                                            plate_name = sampleName[sampleName.index('_P')+2:]

                                            rows=('A','B','C','D','E','F','G','H')
                                            for row in rows:
                                                if row in plate_name: plate_name = plate_name[:plate_name.index(row)]

                                            if plate_name[-1:]=='_': plate_name = plate_name[:-1]
                                            
                                            sampleName2Plate[key] = plate_name
                                        elif sampleName[:8]=='Re_array' or sampleName[:8]=='Re-array':
                                            if '_pl' in sampleName: plate_name = sampleName[sampleName.index('_pl')+3:]
                                            elif '_Pl' in sampleName: plate_name = sampleName[sampleName.index('_Pl')+3:]
                                            elif '_p' in sampleName: plate_name = sampleName[sampleName.index('_p')+2:]
                                            else:
                                                print(sampleName)
                                                quit()

                                            rows=('A','B','C','D','E','F','G','H')
                                            for row in rows:
                                                if row in plate_name: plate_name = plate_name[:plate_name.index(row)]

                                            if plate_name[-1:]=='_': plate_name = plate_name[:-1]
                                            
                                            sampleName2Plate[key] = plate_name
                                        elif fields[2] in called2plate.keys(): sampleName2Plate[key] = called2plate[fields[2]]
                                        else: sampleName2Plate[key] = "Unknown"

# Now iterate through the covid results and output everything...use the metrics.csv
    for results_dir in resultsdirs:
        directories = os.listdir(results_dir)
        for directory in [os.path.join(results_dir, d) for d in directories]:
            if os.path.isdir(directory):
                directory_name = os.path.basename(directory)
                if directory_name[:7]=='result.' and not directory_name[-5:]=='babwe' and not directory_name[-5:]=='ustin': # Ignore Zimbabwe and Justin data
                    files = os.listdir(directory)

                    library_name='Unknown'
                    for file in files:
                        filename = str(file)
                        if filename[-7:]=='.qc.csv':
                            library_name = filename[:-7]
                    
                    for file in files:
                        filename = str(file)
                        if 'metrics' in filename:
                            # Load in the names of consensus sequences
                            consensus_sequence_names=list()
                            dir_name = os.path.basename(directory)
                            if dir_name[:15]=='result.illumina':
                                consensus_files = os.path.join(directory,  'ncovIllumina_sequenceAnalysis_makeConsensus')
                                for consensus_file_path in [os.path.join(consensus_files, x)  for x in os.listdir(consensus_files)]:
                                    consensus_file = os.path.basename(consensus_file_path)
                                    if not os.path.isdir(consensus_file_path):
                                        with open(consensus_file_path) as f:
                                            try:
                                                lines = f.readlines()
                                                if 'G' in lines[1] or 'C' in lines[1] or 'A' in lines[1] or 'T' in lines[1]: consensus_sequence_names.append(str(consensus_file))
                                            except:
                                                pass
                            elif dir_name[:15]=='result.coronahit':
                                sample_dir = os.path.join(direct, 'articNcovNanopore_sequenceAnalysisMedaka_articMinIONMedaka')
                                sample_directories = os.listdir(sample_dir)
                                for sample_directory in sample_directories:
                                    sample_files = os.listdir(os.path.join(sample_directories, sample_directory))
                                    for sample_file in sample_files:
                                        sample_file_name = str(sample_file)
                                        if sample_file_name[-15:] =='consensus.fasta': consensus_sequence_names.append(str(sample_directory))
                            
                            with open(directory + '/' + filename) as f:
                                lines = f.readlines()
                                first_line=True
                                for line in lines:
                                    if first_line==True: first_line=False
                                    else:
                                        fields = line.rstrip().split(',')
                                        central_sample = fields[0]
                                        date = directory[directory.rfind('.'):]
                                        key = central_sample+date[3:9]
                                        if key in sampleName2RunName.keys():
                                            consensus_exists='False'

                                            for consensus_sequence_name in consensus_sequence_names:
                                                if central_sample in consensus_sequence_name:
                                                    consensus_exists='True'

                                            # Use library name from sequencing run name
                                            run_name = sampleName2RunName[key]

                                            if run_name[:6]>'210506': # Use the new library naming convention for anything on/after 7th May
                                                library_name = 'NORW-20' + run_name[:6]

                                            #if not 'BLANK' in central_sample and not central_sample[-3:]=='_NC' and not central_sample[-3:]=='_PC':
                                            csvfile.write(central_sample + ',' + library_name + ',' + run_name + ',' + sampleName2SequencingDate[key] + ',,,,,,,,' + sampleName2Project[key] + ',' + sampleName2Plate[key] + ',' + consensus_exists + ',' + fields[12] + ',' + fields[13] + '\n')
    csvfile.flush()
    csvfile.close()

def update_sample_meta(args):
    client = get_google_session(args.gcredentials)
    sheet = client.open('COGUK_submission_status').get_worksheet(0) # Index from 0, get the second sheet.
    all_values = sheet.get_all_records()
    
    sample2keyValues=dict()
    for key2value in all_values:
        key = str(key2value['central_sample_id']) + str(key2value['library_name'])
        sample2keyValues[key] = key2value

    # Then iterate through the cells for the row and update accordingly
    
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)

    new_rows=list()
    cells_to_update=list()
    first_row=True
    index2key=dict()
    with open('../metasub/gather_plates.csv') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            if first_row==True:
                for i in range(0,len(row)):
                    index2key[i] = row[i]
                first_row=False
            else:
                key = str(row[0]) + str(row[1])
                if key not in sample2keyValues.keys(): 
                    if not str(row[1])=='Unknown': # Add this new row assuming it's not an unknown library type
                        new_rows.append(row)
                else: # This row already exists, check if there is anything to update...
                    old_data = sample2keyValues[key]

                    new_data=dict()
                    for i in range(0,len(row)):
                        if i<len(index2key):
                            new_data[index2key[i]] = row[i]

                    for old_key in old_data.keys():
                        if old_key in new_data.keys() and not str(old_data[old_key])==str(new_data[old_key]):
                            changed=True
                            if isinstance(old_data[old_key], int) and not new_data[old_key]=='':
                                changed = int(old_data[old_key])!=int(new_data[old_key])
                                
                            if changed==True and (isinstance(old_data[old_key], str) and len(old_data[old_key])==0): # Only update blank cells
                                print('Change found for sample [' + key + '] with field [' + old_key + '] [' + str(old_data[old_key]) + '] -> [' + str(new_data[old_key]) + ']')
                                cells_to_update.append(gspread.models.Cell(row=list(sample2keyValues.keys()).index(key)+2, col=list(old_data.keys()).index(old_key)+1, value=new_data[old_key]))

    if len(new_rows)==0:  print('No new rows were added')
    else: print('Adding ' + str(len(new_rows)) + ' rows')

    # Currently the fields are slightly different...delete and push again if the columns are correct
    
    #new_rows=[['new row 1','column2'],['new row 2'],['new row 3']]
    # Now try updating that sheet...
    sheet.resize(len(row_position)) # Trim any whitespace at the end of the sheet...
#    values = [[x] for x in missing_rows_names]
    sheet.append_rows(new_rows)

    if cells_to_update:
        print('Updating values')
        sheet.update_cells(cells_to_update)
    
    all_values = sheet.get_all_records()
    column_position = sheet.row_values(1)
    row_position = sheet.col_values(1)

    print('Update submission sheet has finished')


def summarise_plates(args):
    client = get_google_session(args.gcredentials)
    sheet = client.open('COGUK_submission_status').get_worksheet(0) # Index from 0, get the second sheet.
    all_values = sheet.get_all_records()

    print('plate_name\tsequencing_date\tnumber_of_samples\tpercentage_passed\tpercentage_hc_passed')

    plate2date=dict()
    plate2number_of_samples=dict()
    plate2passes=dict()
    plate2hc_passes=dict()
    
    for key2value in all_values:
        plate_name = str(key2value['plate'])
        sequencing_date = str(key2value['sequencing_date'])
        basic_qc = str(key2value['basic_qc'])
        high_quality_qc = str(key2value['high_quality_qc'])

        if not plate_name in plate2date.keys(): plate2date[plate_name] = set()
        plate2date[plate_name].add(sequencing_date)
        
        if not plate_name in plate2number_of_samples.keys(): plate2number_of_samples[plate_name]=1
        else:  plate2number_of_samples[plate_name]=plate2number_of_samples[plate_name]+1
        
        if basic_qc=='True':
            if not plate_name in plate2passes.keys(): plate2passes[plate_name]=1
            else:  plate2passes[plate_name]=plate2passes[plate_name]+1

        if high_quality_qc=='True':
            if not plate_name in plate2hc_passes.keys(): plate2hc_passes[plate_name]=1
            else:  plate2hc_passes[plate_name]=plate2hc_passes[plate_name]+1

    for plate_name in sorted(plate2date.keys()):
        passes=0
        if plate_name in plate2passes.keys(): passes = plate2passes[plate_name]
        percentage_passes = (passes*100)/plate2number_of_samples[plate_name]

        hc_passes=0
        if plate_name in plate2hc_passes.keys(): hc_passes = plate2hc_passes[plate_name]
        hc_percentage_passes = (hc_passes*100)/plate2number_of_samples[plate_name]

        if plate2number_of_samples[plate_name]>1: print(plate_name + '\t' + str(list(plate2date[plate_name])) + '\t' + str(plate2number_of_samples[plate_name]) + '\t' + "{:.2f}".format(percentage_passes) + '%\t' + "{:.2f}".format(hc_percentage_passes) + '%')

def generate_audit_report(args):
    startDate = args.startdate
    endDate = args.enddate
    
    with open('audit-' + startDate + '-' + endDate + '.csv','w+') as f:
        date2count = dict()
        date2typeCount=dict()
        libraryTypes=set()
        
        client = get_google_session(args.gcredentials)
        sheet = client.open('COGUK_submission_status').get_worksheet(0) # Index from 0, get the second sheet.
        all_values = sheet.get_all_records()

        for key2value in all_values:
            #sequencingDate = str(key2value['sequencing_date'])
            uploadDate = str(key2value['upload_date'])
            libraryType = str(key2value['library_type'])
            centralSampleID = str(key2value['central_sample_id'])
            
            if libraryType=='': libraryType='Unknown'
            if 'BLANK' in centralSampleID or centralSampleID[-3:]=='_NC' or centralSampleID[-3:]=='_PC' or centralSampleID[-3:]=='-NC' or centralSampleID[-3:]=='-PC': libraryType='Controls'

            if (startDate=='' or uploadDate>=startDate) and (endDate=='' or uploadDate<=endDate):
                if not uploadDate in date2count.keys():
                    date2count[uploadDate]=1
                    date2typeCount[uploadDate] = dict()
                else:
                    date2count[uploadDate]=date2count[uploadDate]+1

                typeCount = date2typeCount[uploadDate]
                if not libraryType in typeCount.keys():
                    typeCount[libraryType]=1
                    libraryTypes.add(libraryType)
                else: typeCount[libraryType]=typeCount[libraryType]+1
            
        header='Date,Total'
        for libraryType in sorted(libraryTypes):
            header = header + ',' + libraryType

        f.write(header + '\n')
        total=0
        for date in sorted(date2count.keys()):
            total = total + date2count[date]
            line = str(date) + ',' + str(date2count[date])

            typeCount = date2typeCount[date]

            for libraryType in sorted(libraryTypes):
                    if libraryType in typeCount.keys(): line = line + ',' + str(typeCount[libraryType])
                    else:  line = line + ',0'
                
            f.write(line + '\n')

        f.write('All,' + str(total) + '\n')
        
        f.flush()
        f.close()



def update_upload_date(args):
    client = get_google_session(args.gcredentials)
    sheet = client.open('COGUK_submission_status').get_worksheet(0) # Index from 0, get the second sheet.
    all_values = sheet.get_all_records()
    
    sampleKey2lineNumber=dict()
    lineNumber=2
    columnNumber=5 # Upload date is in the 5th column
    for key2value in all_values:
        key = str(key2value['central_sample_id']).replace('"','') + str(key2value['run_name']).replace('"','')
        sampleKey2lineNumber[key] = lineNumber
        key = str(key2value['central_sample_id']).replace('"','') + str(key2value['library_name']).replace('"','')
        sampleKey2lineNumber[key] = lineNumber
        lineNumber = lineNumber+1

    cells_to_update=list()
    with open('upload-dates.csv') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            key = str(row[0]) + str(row[1])
            if key in sampleKey2lineNumber.keys():
                print(sampleKey2lineNumber[key])
                cells_to_update.append(gspread.models.Cell(row=sampleKey2lineNumber[key], col=columnNumber, value=row[2]))
            else:
                run_name = str(row[1])
                #run_name = run_name[:run_name.rfind('-')]
                key = str(row[0]) + run_name
                if key in sampleKey2lineNumber.keys(): 
                    cells_to_update.append(gspread.models.Cell(row=sampleKey2lineNumber[key], col=columnNumber, value=row[2]))
                else:
                    run_name = run_name[:run_name.rfind('-')]
                    key = str(row[0]) + run_name
                    if key in sampleKey2lineNumber.keys(): 
                        cells_to_update.append(gspread.models.Cell(row=sampleKey2lineNumber[key], col=columnNumber, value=row[2]))
                    else:
                        print('Upload date not found for ' + key)

    if cells_to_update:
        print('Updating values for ' + str(len(cells_to_update)) + ' rows')
        sheet.update_cells(cells_to_update)
    


if __name__ == '__main__':
    start_time = time.time()
    log.setLevel(logging.INFO)
    desc = __doc__.split('\n\n')[0].strip()
    parser = argparse.ArgumentParser(description=desc,epilog=epi)
    subparsers = parser.add_subparsers(help='commands')
    parser.add_argument ('-v', '--verbose', action='store_true', default=False, help='verbose output')
    parser.add_argument('--version', action='version', version='%(prog)s ' + meta.__version__)
    parser.add_argument('--gcredentials', action='store', default='credentials.json', help='Path to Google Sheets API credentials (JSON)')

    gather_parser = subparsers.add_parser('create_csv', help='Create gather_plates.csv')
    gather_parser.add_argument('--datadir', action='store', default='/home/ubuntu/transfer/incoming/QIB_Sequencing',  help='Name of sequencing directory')
    gather_parser.add_argument('--nextseqdirs', action='store', default='Nextseq_1_runs;Nextseq_2_runs',  help='Name of Nextseq directories within the sequencing directory')
    gather_parser.add_argument('--resultdir', action='store', default='/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq',  help='Name of the pipeline results directory')
    gather_parser.set_defaults(func=gather)

    update_sample_parser = subparsers.add_parser('update_from_csv', help='Update sample from gather_plates.csv')
    update_sample_parser.set_defaults(func=update_sample_meta)

    summarise_parser = subparsers.add_parser('summarise_plates', help='Summarise each plate using gather_plates.csv')
    summarise_parser.set_defaults(func=summarise_plates)

    generate_audit_parser = subparsers.add_parser('generate_audit_report', help='Generate audit reports for Gemma Kay')
    generate_audit_parser.add_argument('--startdate', action='store', default='2021-01-01',  help='The start date for the report')
    generate_audit_parser.add_argument('--enddate', action='store', default='2021-03-31',  help='The end date for the report')
    generate_audit_parser.set_defaults(func=generate_audit_report)

    update_upload_date_parser = subparsers.add_parser('update_upload_date', help='Update upload date')
    update_upload_date_parser.set_defaults(func=update_upload_date)
    
    args = parser.parse_args()
    if args.verbose: 
        log.setLevel(logging.DEBUG)
        log.debug( "Executing @ %s\n"  %time.asctime())    
    if hasattr(args, 'func'):
        args.func(args)
    else: 
        parser.print_help()
    if args.verbose: 
        log.debug("Ended @ %s\n"  %time.asctime())
        log.debug('total time in minutes: %d\n' %((time.time() - start_time) / 60.0))
    sys.exit(0)
