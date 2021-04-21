"""
discord_plugin Runs a bot on discord that excutes nrp2cog functions

Requires Discord API code

### CHANGE LOG ### 
2021-01-08 Nabil-Fareed Alikhan <nabil@happykhan.com>
    * Initial build 
"""
import discord
import json 
from nrp2cog import get_bio_metadata, update_our_meta, update_patient_id
from ct_update import get_ct_metadata, update_ct_meta
import json
import logging
from export_phe import export_phe
import asyncio
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from export_lineage import update_pub_info
import argparse
import meta
import time 

chan_id  = 796393522730762260
client = discord.Client()

epi = "Licence: " + meta.__licence__ +  " by " +meta.__author__ + " <" +meta.__author_email__ + ">"
logging.basicConfig()
log = logging.getLogger()


def get_google_session(credentials='credentials.json'):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials, scope)
    client = gspread.authorize(creds)
    return client

def load_config(config="discord.json"):
    config_dict = json.load(open(config))
    return config_dict

def create_error_list(errors):
    errorlist = ''
    total_char = 0 
    for cogid, x in errors.items(): 
        for field, message in x.items():
            this_message = f"{cogid}\t{field}\t{message[0]}\n"
            if len(errorlist) + len(this_message) < 1850:
                errorlist += this_message
            total_char += len(this_message)
    if total_char > 1850:
        errorlist += '\nTOO MANY ERRORS. TRUNCATED.'
    return errorlist

def export_to_phe_func():
    g_session = get_google_session()
    config = json.load(open('export_settings.json'))
    export_phe(g_session, config['temp_dir'], config['sheet_name'], config['export_server'], config['export_username'], config['key_location'])


@client.event
async def on_ready():
    channel = client.get_channel(chan_id)
    await channel.send('NRP2COG Service started...')    
    print('We have logged in as {0.user}'.format(client))

@client.event
async def on_error():
    channel = client.get_channel(chan_id)
    await channel.send('NRP2COG Service has encountered an error!')    

import random 
@client.event
async def on_message(message):
    channel = client.get_channel(chan_id)

    if message.author == client.user:
        return

    if message.content.startswith('!wisdom'):
        wise = random.choice(open('proverbs.txt').readlines())
        await message.channel.send(f"```\n{wise.strip()}\n```\n")

    if message.content.startswith('!ping'):
        await message.channel.send('Pong!')

    if message.content.startswith('!update_metadata'):
        await message.channel.send('Updating metadata...')
        log.info('Updating metadata')
        g_session = get_google_session()
        new_dict, errors = get_bio_metadata(g_session)
        if errors:
            errorlist = create_error_list(errors)
            await message.channel.send(f'Following {len(errors)} errors were found in the input sheet:\n```\n{errorlist}\n```\n These records have been ignored')

        else:
            await message.channel.send('No errors from input sheet')
        error_messages = update_our_meta(new_dict, g_session, force_update = False)
        if error_messages:
            for error_message in error_messages:
                if len(error_message) >= 1950:
                    trun_message = '\nTOO MANY ERRORS. TRUNCATED.'
                    trun_len = 1950 - len(trun_message)
                    error_message = error_message[0:trun_len] + trun_message
                await message.channel.send("```\n" + error_message + "```\n" )
        # Create Patients field
        update_patient_id(g_session)
        await message.channel.send('Updated metadata.')
        log.info('Done metadata')

    if message.content.startswith('!update_ct'):
        await message.channel.send('Updating ct data...')
        log.info('Updating ct data')
        g_session = get_google_session()
        new_dict, errors = get_ct_metadata(g_session)
        if errors:
            errorlist = create_error_list(errors)
            error_message = f'Following {len(errors)} errors were found in the input sheet:\n```\n{errorlist}\n```\n These records have been ignored'
            if len(error_message) < 2000:
                await message.channel.send(error_message)
            else:
                await message.channel.send(f' {len(errors)} errors were found in the input sheet.  These records have been ignored')
        else:
            await message.channel.send('No errors from input sheet')
        error_messages = update_ct_meta(new_dict, g_session)     

        if error_messages:
            for error_message in error_messages:
                if len(error_message) >= 1950:
                    trun_message = '\nTOO MANY ERRORS. TRUNCATED.'
                    trun_len = 1950 - len(trun_message)
                    error_message = error_message[0:trun_len] + trun_message
                await message.channel.send("```\n" + error_message + "```\n" )
        else:
            await message.channel.send('No Errors for updating')    
        await message.channel.send('Updated ct data.')
        log.info('Done ct data')    

    if message.content.startswith('!export_lineages'):
        g_session = get_google_session()
        update_pub_info(g_session)
        await message.channel.send('Exported Sample lineage information.')

    if message.content.startswith('!export_to_server'):
        export_to_phe_func()
        await message.channel.send('Exported lab IDs to PHE.')

    if message.content.startswith('!help'):
        help_message = 'Hi, This is the NRP2COG bot\nI am designed to import/export data from the SARSCOV2-Metadata master table\nI have the following options:\n'
        help_message += '    !help: This message\n'
        help_message += '    !ping: Replies with pong, so you can check the service is running\n'
        help_message += '    !update_metadata: Imports metadata from the raw input table\n'
        help_message += '    !update_ct: Imports ct data\n'
        help_message += '    !export_lineages: Exports Sample information and lineages\n'
        help_message += '    !export_to_server: Exports Lab and COG IDs to PHE, so they can pair them\n'
        help_message += '    !wisdom: Replies something meaningful\n'        
        await message.channel.send(f"```\n{help_message}\n```")

@client.event
async def bg_phe_export():
    await client.wait_until_ready()
    while not client.is_closed():
        export_to_phe_func()
        channel = client.get_channel(chan_id)
        g_client = get_google_session()
        new_dict, errors = get_bio_metadata(g_client)
        update_our_meta(new_dict, g_client, force_update = False)
        # Create Patients field
        update_patient_id(g_client)        
        await channel.send('Updated metadata & Exported lab IDs to PHE.')        
        await asyncio.sleep(86400) # task runs every day

def main(args):
    config = load_config()
    chan_id = args.chanid
    log.setLevel(logging.INFO)
    bg_task = client.loop.create_task(bg_phe_export())
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
