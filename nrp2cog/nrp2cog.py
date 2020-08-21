"""
nrp2cog converts the meta from nrp to cog uk (all in google sheets)

Requires login for google sheets

### CHANGE LOG ### 
2020-08-17 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build - split from dirty scripts
"""
import logging
import time
import argparse
import sys
import meta
from nrp_util import get_google_session
from update_surv import get_surv_metadata, update_surv_meta
from export_lineage import update_pub_info
from update_lineage import get_lineage_metadata, update_lineage_meta
from ct_update import get_ct_metadata, update_ct_meta
from update_metadata import get_bio_metadata, update_our_meta, update_patient_id


epi = "Licence: " + meta.__licence__ +  " by " +meta.__author__ + " <" +meta.__author_email__ + ">"
logging.basicConfig()
log = logging.getLogger()


def update_surv_option(args):
    # TODO: Add to options
    # Update surveliannce 
    client = get_google_session(args.gcredentials)
    new_dict, surv_counts = get_surv_metadata(client)
    update_surv_meta(new_dict, surv_counts, client)


def export_lineage_option(args):

    # Build def info:
    client = get_google_session(args.gcredentials)
    update_pub_info(client)

def update_lineage_option(args):

    client = get_google_session(args.gcredentials)
    new_dict = get_lineage_metadata(client, sheet_name=args.lineagedata)
    update_lineage_meta(new_dict, client)    

def update_metadata_option(args):

    client = get_google_session(args.gcredentials)
    new_dict = get_bio_metadata(client)
    update_our_meta(new_dict, client, force_update = False)
    # Create Patients field
    update_patient_id(client)

def update_ct_option(args):

    client = get_google_session(args.gcredentials)
    new_dict = get_ct_metadata(client, sheet_name=args.ctdata)
    update_ct_meta(new_dict, client, sheet_name=args.maindata)

    
if __name__ == '__main__':
    start_time = time.time()
    log.setLevel(logging.INFO)
    desc = __doc__.split('\n\n')[0].strip()
    parser = argparse.ArgumentParser(description=desc,epilog=epi)
    subparsers = parser.add_subparsers(help='commands')
    parser.add_argument ('-v', '--verbose', action='store_true', default=False, help='verbose output')
    parser.add_argument('--version', action='version', version='%(prog)s ' + meta.__version__)
    parser.add_argument('--gcredentials', action='store', default='credentials.json', help='Path to Google Sheets API credentials (JSON)')
    parser.add_argument('--maindata', action='store', default='SARCOV2-Metadata',  help='Name of Master Table in Google sheets')

    # CT Parser
    ct_parser = subparsers.add_parser('ct_update', help='Update CT values')
    ct_parser.add_argument('--ctdata', action='store', default='cov-ct',  help='Name of CT table in Google sheets')
    ct_parser.set_defaults(func=update_ct_option)

    # Update lineage parser
    lineage_parser = subparsers.add_parser('lineage_update', help='Update Lineage values')
    lineage_parser.add_argument('--lineagedata', action='store', default='peroba',  help='Name of sample lineages table in Google sheets')
    lineage_parser.set_defaults(func=update_lineage_option)

    # Export lineage parser
    export_parser = subparsers.add_parser('export_lineage', help='Export Lineage values')
    export_parser.add_argument('--exportdata', action='store', default='Sample-lineages',  help='Name of sample lineages table in Google sheets')
    export_parser.set_defaults(func=export_lineage_option)

    # Update metadata parser
    meta_parser = subparsers.add_parser('update_metadata', help='Update metadata')
    meta_parser.add_argument('--metadata', action='store', default='COG_UK_Metadata_QIB_Deidentified',  help='Name of Master Table in Google sheets')
    meta_parser.set_defaults(func=update_metadata_option)

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