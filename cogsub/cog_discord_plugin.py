"""
discord_plugin Runs a bot on discord that excutes cogsub functions

Requires Discord API code

### CHANGE LOG ### 
2021-01-08 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build 
"""
import discord
import json 
import json
import logging
import asyncio
import gspread
import argparse
import meta
import time 
import os 
import csv
from cogsub import cogsub_run

chan_id  = 796393522730762260
client = discord.Client()

epi = "Licence: " + meta.__licence__ +  " by " +meta.__author__ + " <" +meta.__author_email__ + ">"
logging.basicConfig()
log = logging.getLogger()

run_directories = ["/home/ubuntu/transfer/incoming/QIB_Sequencing/Nextseq_2_runs", "/home/ubuntu/transfer/incoming/QIB_Sequencing/Nextseq_1_runs"]
data_directories = ['/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq']


def load_config(config="discord.json"):
    config_dict = json.load(open(config))
    return config_dict

@client.event
async def on_ready():
    channel = client.get_channel(chan_id)
    await channel.send('COG Submission Service started...')    
    print('We have logged in as {0.user}'.format(client))

@client.event
async def on_error():
    channel = client.get_channel(chan_id)
    await channel.send('COG Submission Service has encountered an error!')    

def directory_dict(data_directories):
    all_dirs =  []
    for direct in data_directories:
        all_dirs += [os.path.join(direct, x) for x in os.listdir(direct) if os.path.isdir(os.path.join(direct, x ))] 
    all_dirs.sort(key=os.path.getctime)
    dir_dict = {} 
    count = 1 
    for x in all_dirs:
        dir_dict[count] = dict(dirname = os.path.basename(x), path = x)
        count += 1
    return dir_dict
 
def format_directory_list(data_directories):
    dir_dict = directory_dict(data_directories)
    list_box = [] 
    message_string = ''
    total_char = 0 
    for no, values in dir_dict.items(): 
        if total_char > 1800:
            list_box.append(message_string)
            message_string = '' 
            total_char = 0 
        this_message = f"{no}\t{values['dirname']}\n"
        message_string += this_message
        total_char += len(this_message)
    if message_string != '':
        list_box.append(message_string)
    return list_box

@client.event
async def on_message(message):
    channel = client.get_channel(chan_id)

    if message.author == client.user:
        return
  
    if message.content.startswith('!cogsub_ping'):
        await message.channel.send('Pong! (COG Submission Service)')

    if message.content.startswith('!cogsub_list_runs'):
        await message.channel.send("Here are all the runs:")
        for text in format_directory_list(run_directories):
            await message.channel.send(f"```\n{text}\n```\n")

    if message.content.startswith('!cogsub_list_datadirs'):
        await message.channel.send("Here are all the output directories:")
        for text in format_directory_list(data_directories):
            await message.channel.send(f"```\n{text}\n```\n")

    if message.content.startswith('!cogsub_update_run_metadata'):
        datadir_number = int(message.content.split()[1])
        data_dict = directory_dict(data_directories)
        await message.channel.send(f"```\nNot Implemented\n```\n")
        # cogsub_run('majora.json', data_dict[datadir_number]["path"], run_dict[run_number]["dirname"], 'SARCOV2-Metadata', False, False, dry=False)

    if message.content.startswith('!cogsub_upload_to_cog'):
        datadir_number = int(message.content.split()[1])
        run_number = int(message.content.split()[2])
        run_dict = directory_dict(run_directories)
        data_dict = directory_dict(data_directories)
        sample_sheet_path = os.path.join(run_dict[run_number]['path'], 'SampleSheet.csv')
        if os.path.exists(sample_sheet_path):
            all_lines = [ x.strip() for x in  open(sample_sheet_path).readlines() ] 
            header = [n for n,l in enumerate(all_lines) if l.startswith('Sample_name')][0]
            samples = all_lines[header:]
            text = f'Are you sure you want to upload output dir {data_dict[datadir_number]["dirname"]} with run name as {run_dict[run_number]["dirname"]}'
            await message.channel.send(f"```\n{text}\n```\n")
            plate_info = {}
            plate_text = ''
            for x in csv.DictReader(samples):
                if x['Project'].upper() == 'COG':
                    if plate_info.get(x['Called']):
                        plate_info[x['Called']].append(x['Sample_name'])
                    else:
                        plate_info[x['Called']] = [x['Sample_name']] 
            if len(plate_info) > 0 : 
                for x,y in plate_info.items():
                    plate_text += f'{x}\t' + ','.join(y[-10:])  + '\n'
                await message.channel.send(f"```\nThe following plates have been detected:\n{plate_text}\n```\n")
                try:
                    def is_guy(m):
                        return m.author == 'happykhan'
                    msg  = await client.wait_for('message', timeout=10.0, check=is_guy)
                    if msg.content == 'all':
                        plates = plate_info.keys()
                    else:
                        plates = msg.content.split(',')
                    with open(os.path.join(data_dict[datadir_number]["path"], 'uploadlist'), 'w') as uploadlist:
                        for x,y in plate_info.items():
                            if x in plates: 
                                uploadlist.write('\n'.join(y))                
                    cogsub_run('majora.json', data_dict[datadir_number]["path"], run_dict[run_number]["dirname"], 'SARCOV2-Metadata', False, False, dry=False)
                except asyncio.TimeoutError:
                    await message.channel.send(f"```\Didn't hear a response. Reply faster. Aborting upload\n```\n")    
                except:
                    await message.channel.send(f"```\nSomething bad happened. Aborting upload\n```\n")    
                    logging.exception('Failed to read input from upload ') 
            else:
                await message.channel.send(f"```\nNo COG plates detected in SampleSheet. See {sample_sheet_path}. Aborting upload\n```\n")    
        else:
            await message.channel.send(f"```\nThere is no sample sheet in {sample_sheet_path}. Are you sure you selected the right folder?\n```\n")
        

    if message.content.startswith('!cogsub_help'):
        help_message = 'Hi, This is the COG Submission bot\nI am designed to upload data to CLIMB\nI have the following options:\n'
        help_message += '    !cogsub_help: This message\n'
        help_message += '    !cogsub_ping: Replies with pong, so you can check the service is running\n'
        help_message += '    !cogsub_list_runs: Lists sequencing runs\n'
        help_message += '    !cogsub_list_datadirs: List output directories\n'
        help_message += '    !cogsub_upload_to_cog: Uploads data to CLIMB. Needs datadir number and run number\n'
        await message.channel.send(f"```\n{help_message}\n```")


def main(args):
    config = load_config()
    chan_id = args.chanid
    log.setLevel(logging.INFO)
    if config.get('discord_token'):
        client.run(config.get('discord_token'))
    else:
        log.error('No token found')

if __name__ == '__main__':
    start_time = time.time()
    log.setLevel(logging.INFO)
    desc = __doc__.split('\n\n')[0].strip()
    parser = argparse.ArgumentParser(description=desc,epilog=epi)
    parser.add_argument ('-v', '--verbose', action='store_true', default=False, help='verbose output')
    parser.add_argument('--version', action='version', version='%(prog)s ' + meta.__version__)
    parser.add_argument('--config', action='store', default='discord.json', help='Path to Discord credentials (JSON)')
    parser.add_argument('--chanid', action='store', default=796393522730762260, help='ID of channel to post')

    args = parser.parse_args()
    if args.verbose: 
        log.setLevel(logging.DEBUG)
        log.debug( "Executing @ %s\n"  %time.asctime())    
    main(args)
    if args.verbose: 
        log.debug("Ended @ %s\n"  %time.asctime())
        log.debug('total time in minutes: %d\n' %((time.time() - start_time) / 60.0))
