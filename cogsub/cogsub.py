"""
cogsub submits metadata and sequences to COG server 

Requires login for google sheets

### CHANGE LOG ### 
2020-08-17 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build - split from dirty scripts
"""
from sync_data import cogsub_sync
import logging
import time
import sys
from submit_data import cogsub_submit, cogsub_make_sheet 
import meta
import argparse

epi = "Licence: " + meta.__licence__ +  " by " +meta.__author__ + " <" +meta.__author_email__ + ">"
logging.basicConfig()
log = logging.getLogger()


def submit_parser_option(args):
    cogsub_submit(args.majora_token, args.datadir, args.runname, args.sheet_name,  args.gcredentials, args.ont)
    
def sync_parser_option(args):
    cogsub_sync(args.majora_token, args.sheet_name,  args.gcredentials)
   
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
    parser.add_argument('--majora_token', action='store', default='majora.json', help='Path to MAJORA COG API credentials (JSON)')
    
    # Submit parser
    submit_parser = subparsers.add_parser('submit', help='Submit new data to COG')
    submit_parser.add_argument('datadir', action='store', help='Location of ARTIC pipeline output')
    submit_parser.add_argument('runname', action='store', help='Sequencing run name, must be unique')
    submit_parser.add_argument('--ont', action='store_true', default=False, help='Is the output directory from nanopore')
    submit_parser.set_defaults(func=submit_parser_option)
    
    # Sync parser
    sync_parser = subparsers.add_parser('sync', help='Sync existing data to COG')
    sync_parser.set_defaults(func=sync_parser_option)

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

