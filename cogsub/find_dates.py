"""
find_dates between a given date range 

find_dates between a given date range 

### CHANGE LOG ### 
2021-02-15 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build 
"""
import logging 
import time 
import argparse
import pathlib

epi = "Licence: GPLv3 by Nabil-Fareed Alikhan <nabil@happykhan.com>"
__version__ = "0.0.1"
logging.basicConfig()
log = logging.getLogger()
import os 
from datetime import date

def main(args):
    for x in os.listdir(args.directory): # Run dir 
        for sample_dir in os.listdir(os.path.join(args.directory, x) ): # Sample dir 
            current_file = pathlib.Path(os.path.join(args.directory, x, sample_dir) )
            start_date = date.fromisoformat(args.start_date)
            end_date = date.fromisoformat(args.end_date)
            ctime = date.fromtimestamp(current_file.stat().st_ctime)
            if ctime >= start_date and ctime <= end_date:
                print(f'{sample_dir},x,{ctime}')


    

if __name__ == '__main__':
    start_time = time.time()
    log.setLevel(logging.INFO)
    desc = __doc__.split('\n\n')[0].strip()
    parser = argparse.ArgumentParser(description=desc,epilog=epi)
    # Main parameters 
    parser.add_argument ('-v', '--verbose', action='store_true', default=False, help='verbose output')
    parser.add_argument('--version', action='version', version='%(prog)s ' + __version__)
    parser.add_argument('directory', action='store', help='directory to search')
    parser.add_argument('--start_date', action='store', default='2020-10-01', help='Starting date range')    
    parser.add_argument('--end_date', action='store', default='2021-01-08', help='Ending date range')

    args = parser.parse_args()
    main(args)  
    if args.verbose: 
        log.setLevel(logging.DEBUG)
        log.debug( "Executing @ %s\n"  %time.asctime())    
    if args.verbose: 
        log.debug("Ended @ %s\n"  %time.asctime())
        log.debug('total time in minutes: %d\n' %((time.time() - start_time) / 60.0))

