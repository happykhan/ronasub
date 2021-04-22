"""
metasub submits sequences to COG server and creates metadata sheets for interactive submission

Requires login for google sheets

### CHANGE LOG ### 
2021-04-19 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build - split from cogsub 
"""
import logging
import time
import sys
import meta
import argparse
from gather_plates import gather
from check_meta import check_meta
from submit_filedata import legacy_submit_filedata
from generate_metasheet import generate_metasheet

epi = "Licence: " + meta.__licence__ +  " by " +meta.__author__ + " <" +meta.__author_email__ + ">"
logging.basicConfig()
log = logging.getLogger()

def plates_parser_option(args):
    # Reads output dir and fetches samples with plate info 
    gather(args.majora_token, args.datadir, args.runname, args.sheet_name,  args.gcredentials, args.ont)

def sync_meta_option(args):
    # Checks local metadata with COG metadata is consistent 
    check_meta(args.majora_token, args.sheet_name, args.submission_sheet, args.gcredentials)

def legacy_submit_filedata_option(args):
    # Sends files from sequencing run to COG 
    legacy_submit_filedata(args.datadir, args.run_name, args.majora_token)

def generate_metasheet_option(args):
    # Generates metadata sheet for submission. 
    generate_metasheet()

if __name__ == '__main__':
    start_time = time.time()
    log.setLevel(logging.INFO)
    desc = __doc__.split('\n\n')[0].strip()
    parser = argparse.ArgumentParser(description=desc,epilog=epi)
    subparsers = parser.add_subparsers(help='commands')
    # Main parameters 
    parser.add_argument ('-v', '--verbose', action='store_true', default=False, help='verbose output')
    parser.add_argument('--version', action='version', version='%(prog)s ' + meta.__version__)
    parser.add_argument('--gcredentials', action='store', default='credentials.json', help='Path to Google Sheets API credentials (JSON)')
    parser.add_argument('--sheet_name', action='store', default='SARCOV2-Metadata', help='Name of Master Table in Google sheets')    
    parser.add_argument('--submission_sheet', action='store', default='COGUK_submission_status', help='Name of Table tracking submission status in Google sheets')     
    
    # Plates parser
    plates_parser = subparsers.add_parser('check_plates', help='Reads output dir and fetches samples with plate info ')
    plates_parser.add_argument('datadir', action='store', help='Location of all sequencing data')    
    plates_parser.set_defaults(func=plates_parser_option)

    # Sync parser
    sync_parser = subparsers.add_parser('check_sync', help='Checks local metadata with COG metadata is consistent')
    sync_parser.add_argument('--ont', action='store_true', default=False, help='Is the output directory from nanopore')
    sync_parser.add_argument('--majora_token', action='store', default='majora.json', help='Path to MAJORA COG API credentials (JSON)')
    sync_parser.set_defaults(func=sync_meta_option)
    
    # LEGACY Submit parser
    legacy_submit_filedata_parser = subparsers.add_parser('legacy_submit_files', help='Sends files from sequencing run to COG ')
    legacy_submit_filedata_parser.add_argument('datadir', action='store', help='Location of data output; will ignore google sheet')
    legacy_submit_filedata_parser.add_argument('run_name', action='store', help='Run name to submit')
    legacy_submit_filedata_parser.add_argument('--majora_token', action='store', default='majora.json', help='Path to MAJORA COG API credentials (JSON)')
    legacy_submit_filedata_parser.set_defaults(func=legacy_submit_filedata_option)

    # Metasheet parser 
    generate_metasheet_parser = subparsers.add_parser('generate_sheet', help='Generates metadata sheet for submission')
    
    generate_metasheet_parser.set_defaults(func=generate_metasheet_option)

    args = parser.parse_args()
    if hasattr(args, 'func'):
        args.func(args)
    else: 
        parser.print_help()    
    if args.verbose: 
        log.setLevel(logging.DEBUG)
        log.debug( "Executing @ %s\n"  %time.asctime())    
    if args.verbose: 
        log.debug("Ended @ %s\n"  %time.asctime())
        log.debug('total time in minutes: %d\n' %((time.time() - start_time) / 60.0))

