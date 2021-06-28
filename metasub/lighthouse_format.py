"""
lighthouse_format formats data from lighthouse labs into a format suitable for upload to http://metadata.cog-uk.io/



### CHANGE LOG ### 
2021-06-28 Martin Lott <martin.lott@quadram.ac.uk>
    * Initial build - moved standalone scripts
"""


"""
it should 
* This takes as input data in tsv format from Lighthouse labs e.g. Brants Bridge and ICHNE
* Use Google Sheets to convert Excel to TSV format to avoid errors.
* Outputs data in csv format which can be uploaded through 
* Example command: python3 lighthouse_format.py > meta.csv """


import logging
import time
import argparse
import sys
import meta

epi = "Licence: " + meta.__licence__ +  " by " +meta.__author__ + " <" +meta.__author_email__ + ">"
logging.basicConfig()
log = logging.getLogger()


def output_brants_bridge_metadata(args):
    print('output_brants_bridge_metadata')
    plateId = args.plateid
    libraryName = args.libraryname
    receivedDate = args.receiveddate

    rootSample2CogId=dict()

    with open('brants-bridge-map-updated.csv') as f:
        lines = f.readlines()
        for line in lines:
            fields = line.rstrip().split(',')
            if fields[1]==plateId:
                rootSample2CogId[fields[0]]=fields[2]


    plate2date=dict()
    with open('plate2date.tsv') as f:
        lines = f.readlines()
        for line in lines:
            fields = line.rstrip().split('\t')
            plate2date[fields[0]] = fields[1]


    print('central_sample_id,adm1,received_date,collection_date,source_age,source_sex,is_surveillance,collection_pillar,is_hcw,employing_hospital_name,employing_hospital_trust_or_board,is_hospital_patient,is_icu_patient,admitted_with_covid_diagnosis,admission_date,admitted_hospital_name,admitted_hospital_trust_or_board,is_care_home_worker,is_care_home_resident,anonymised_care_home_code,adm2,adm2_private,biosample_source_id,root_sample_id,sender_sample_id,collecting_org,sample_type_collected,sample_type_received,swab_site,epi_cluster,investigation_name,investigation_site,investigation_cluster,majora_credit,ct_1_ct_value,ct_1_test_target,ct_1_test_platform,ct_1_test_kit,ct_2_ct_value,ct_2_test_target,ct_2_test_platform,ct_2_test_kit,sequencing_org_received_date,library_name,library_seq_kit,library_seq_protocol,library_layout_config,library_selection,library_source,library_strategy,library_layout_insert_length,library_layout_read_length,barcode,artic_primers,artic_protocol,run_name,instrument_make,instrument_model,start_time,end_time,flowcell_id,flowcell_type,bioinfo_pipe_name,bioinfo_pipe_version')
            
    with open('tsv/' + plateId + '.tsv') as f:
        lines = f.readlines()
        for line in lines:
            if not line[:5]=='Plate' and not line[:4]=='Root':
                fields = line.rstrip().split('\t')
                rootSample = fields[0]
                if len(rootSample)>0 and not rootSample[0]=='-' and not rootSample[0]==' ':
                    ct1value=fields[4]
                    if ct1value=='-1': ct1value=''

                    ct2name='N'
                    ct2value=fields[5]
                    if ct2value=='-1': # Use the S gene instead if no CT value for the N gene.
                        ct2name='S'
                        ct2value=fields[6]
                    
                    
                    print(rootSample2CogId[rootSample] + ',UK-ENG,' + receivedDate +  ',' + plate2date[plateId] + ',,,Y,2,,,,,,,,,,,,,,,' + rootSample + ',' + rootSample + ',' + rootSample + ',BRANTS BRIDGE,,,,,,,,,' + ct1value + ',ORF1AB,,,' +  ct2value + ',' + ct2name + ',,,' + receivedDate + ',' + libraryName + ',Nextera,Nextera LITE,PAIRED,PCR,VIRAL_RNA,AMPLICON,,,,,,' + libraryName + ',ILLUMINA,NextSeq 500,,,,,,')



def output_ichne_metadata(args):
    libraryName = args.libraryname
    receivedDate = args.receiveddate

    print('central_sample_id,adm1,received_date,collection_date,source_age,source_sex,is_surveillance,collection_pillar,is_hcw,employing_hospital_name,employing_hospital_trust_or_board,is_hospital_patient,is_icu_patient,admitted_with_covid_diagnosis,admission_date,admitted_hospital_name,admitted_hospital_trust_or_board,is_care_home_worker,is_care_home_resident,anonymised_care_home_code,adm2,adm2_private,biosample_source_id,root_sample_id,sender_sample_id,collecting_org,sample_type_collected,sample_type_received,swab_site,epi_cluster,investigation_name,investigation_site,investigation_cluster,majora_credit,ct_1_ct_value,ct_1_test_target,ct_1_test_platform,ct_1_test_kit,ct_2_ct_value,ct_2_test_target,ct_2_test_platform,ct_2_test_kit,sequencing_org_received_date,library_name,library_seq_kit,library_seq_protocol,library_layout_config,library_selection,library_source,library_strategy,library_layout_insert_length,library_layout_read_length,barcode,artic_primers,artic_protocol,run_name,instrument_make,instrument_model,start_time,end_time,flowcell_id,flowcell_type,bioinfo_pipe_name,bioinfo_pipe_version')
            
    with open('NCL-LHL-COG-COLLECTION-220621-with-COG-ID - NORWICH-220621-5.tsv') as f:
        lines = f.readlines()
        for line in lines:
                fields = line.rstrip().split('\t')
                if len(fields)>14:
                    rootSample = fields[1]
                    if len(rootSample)==11:
                        ct1value=fields[8]
                        if ct1value=='-1': ct1value=''

                        ct2name='N'
                        ct2value=fields[6]
                        if ct2value=='-1': # Use the S gene instead if no CT value for the N gene.
                            ct2name='S'
                            ct2value=fields[7]
                        
                        collectionDate = fields[3]
                        collectionDate = collectionDate[6:10] + '-' + collectionDate[3:5] + '-' + collectionDate[0:2]
                        
                        print(fields[14] + ',UK-ENG,' + receivedDate +  ',' + collectionDate + ',,,Y,2,,,,,,,,,,,,,,,' + rootSample + ',' + rootSample + ',' + rootSample + ',ICHNE,,,,,,,,,' + ct1value + ',ORF1AB,,,' +  ct2value + ',' + ct2name + ',,,' + receivedDate + ',' + libraryName + ',Nextera,Nextera LITE,PAIRED,PCR,VIRAL_RNA,AMPLICON,,,,,,' + libraryName + ',ILLUMINA,NextSeq 500,,,,,,')


if __name__ == '__main__':
    start_time = time.time()
    log.setLevel(logging.INFO)
    desc = __doc__.split('\n\n')[0].strip()
    parser = argparse.ArgumentParser(description=desc,epilog=epi)
    subparsers = parser.add_subparsers(help='commands')
    parser.add_argument ('-v', '--verbose', action='store_true', default=False, help='verbose output')
    parser.add_argument('--version', action='version', version='%(prog)s ' + meta.__version__)

    brants_bridge_parser = subparsers.add_parser('brants_bridge', help='Create the meta data for a Brants Bridge plate')
    brants_bridge_parser.add_argument('--plateid', action='store', default='95040456000323697',  help='Name of sequencing directory')
    brants_bridge_parser.add_argument('--libraryname', action='store', default='NORW-20210622',  help='Name of sequencing library')
    brants_bridge_parser.add_argument('--receiveddate', action='store', default='2021-06-08',  help='The date we received the samples')
    brants_bridge_parser.set_defaults(func=output_brants_bridge_metadata)

    ichne_parser = subparsers.add_parser('ichne', help='Create the meta data for a ICHNE samples')
    ichne_parser.add_argument('--libraryname', action='store', default='NORW-20210622',  help='Name of sequencing library')
    ichne_parser.add_argument('--receiveddate', action='store', default='2021-06-21',  help='The date we received the samples')
    ichne_parser.set_defaults(func=output_ichne_metadata)

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
