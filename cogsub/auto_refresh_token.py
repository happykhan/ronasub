"""
refresh_token Script to automatically bump the majora token

Requires login for majora

### CHANGE LOG ### 
2020-08-17 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build - split from dirty scripts
"""
import time 
import argparse
import logging
from majora_util import majora_oauth, oauth_load_tokens
import meta
from majora_endpoints import ENDPOINTS
from cogsub_util import load_config

epi = "Licence: " + meta.__licence__ +  " by " +meta.__author__ + " <" +meta.__author_email__ + ">"
logging.basicConfig()
log = logging.getLogger()

def no_scope():
    endpoints = ["api.artifact.biosample.add", "api.artifact.library.add", "api.process.sequencing.add"]
    scopes = [] 
    for endpoint in endpoints:
        scopes.append(ENDPOINTS[endpoint]['scope']) 
    return scopes 

def do_oauth_refresh(majora_config):

    scopes  = oauth_load_tokens()
    if not scopes:
        scopes = no_scope()

    for scope in scopes:
        oauth_session, oauth_token = majora_oauth(load_config(majora_config), scope, force_refresh=True)
        if oauth_token:
            print("Token with scope '%s' refreshed successfully" % scope)

def wrap_oauth_refresh(args):
    
    do_oauth_refresh(args.majora_config)

if __name__ == '__main__':
    start_time = time.time()
    log.setLevel(logging.INFO)
    desc = __doc__.split('\n\n')[0].strip()
    parser = argparse.ArgumentParser(description=desc,epilog=epi)
    parser.add_argument ('-v', '--verbose', action='store_true', default=False, help='verbose output')
    parser.add_argument('--version', action='version', version='%(prog)s ' + meta.__version__)
    parser.add_argument('--majora_token', action='store', default='.cogsub-tokens', help='Path to MAJORA COG API credentials (JSON)')
    parser.add_argument('--majora_config', action='store', default='majora.json', help='Path to MAJORA COG config (JSON)')
    args = parser.parse_args()
    if args.verbose: 
        log.setLevel(logging.DEBUG)
        log.debug( "Executing @ %s\n"  %time.asctime())    
    wrap_oauth_refresh(args)
    if args.verbose: 
        log.debug("Ended @ %s\n"  %time.asctime())
        log.debug('total time in minutes: %d\n' %((time.time() - start_time) / 60.0))
