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
from discord.ext import commands, tasks
from gather_plates import gather
from check_meta import check_meta as check_meta_func
from submit_filedata import submit_filedata as submit_filedata_func
from generate_metasheet import generate_metasheet
import shutil
from discord.ext.commands.errors import MissingRequiredArgument

chan_id  = 796393522730762260
bot = commands.Bot(command_prefix='$')


epi = "Licence: " + meta.__licence__ +  " by " +meta.__author__ + " <" +meta.__author_email__ + ">"
logging.basicConfig()
log = logging.getLogger()


class Back(commands.Cog):

    def __init__(self, bot):
        self.bot= bot
        self._batch = []
        self.lock = asyncio.Lock()
        self.bg_update_submission_table.start()
        
    @tasks.loop(seconds=600)
    async def bg_update_submission_table(self):
        channel = bot.get_channel(chan_id)
        await channel.send('Running submission update daemon ... ')
        gather()
        channel = bot.get_channel(chan_id)
        check_meta_func('majora.json', 'COGUK_submission_status', 'credentials.json')
        await channel.send('Updated from sequencing folders and updated submission table')        

    @bg_update_submission_table.before_loop
    async def before_bg_update_submission_table(self):
        logging.info('waiting...')
        await self.bot.wait_until_ready()        



def load_config(config="discord.json"):
    config_dict = json.load(open(config))
    return config_dict


@bot.event
async def on_ready():
    channel = bot.get_channel(chan_id)
 #   await channel.send('COG Submission Service started...')    
    print('We have logged in as {0.user}'.format(bot))

@bot.event
async def on_error():
    channel = bot.get_channel(chan_id)
    await channel.send('COG Submission Service has encountered an error!')

@bot.command(pass_context=True, brief="Replies with Pong!", help="Replies with pong, so you can check the service is running")
async def ping(ctx):
    await ctx.send('Pong! (COG Submission Service)')

@bot.command(pass_context=True, brief="Scans seq dirs and updates submission table", help="Reads through all the sequencing output directories and creates new records in the submisison table if required" )
async def gather_plates(ctx):
    await ctx.send(f"```\nUpdating submission table...\n```\n")
    gather()
    await ctx.send(f"```\nUpdated submission table\n```\n")

@bot.command(pass_context=True, brief="Scans COG info and updates submission table", help="Fetches upload status for all records and updates their status in the submission table" )
async def check_meta(ctx):
    await ctx.send(f"```\nUpdating submission table...\n```\n")
    check_meta_func('majora.json', 'COGUK_submission_status', 'credentials.json')
    await ctx.send(f"```\nUpdated submission table\n```\n")    

@bot.command(pass_context=True, brief="Sends seq data to COG", help="Sends seq from given plates from a given datadir" )
async def submit_filedata(ctx, datadir, library_type, plate_list, run_name=None):
    await ctx.send(f"```\nSending files to COG...\n```\n")
    datadir = os.path.join("/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/", datadir)
    submit_filedata_func(datadir, 'credentials.json', 'majora.json', 'COGUK_submission_status', library_type, plate_list, run_name=run_name)
    await ctx.send(f"```\nSent files to COG\n```\n")

@bot.command(pass_context=True, brief="Creates sample sheet for upload", help="Creates a sample sheet read for upload through COG interactive submission\nUSAGE: make_samplesheet <datadir_name> <library_type> <list_of_plates>" )
async def make_samplesheet(ctx, datadir, library_type, plate_list, run_name=None):
    datadir = os.path.join("/home/ubuntu/transfer/incoming/QIB_Sequencing/Covid-19_Seq/", datadir)
    if library_type.upper() in ['REACT']:
        sheet_name = 'SARSCOV2-REACT-Metadata'
    else: 
        sheet_name = 'SARCOV2-Metadata'
    await ctx.send("```\nReading sample sheets...\n```\n")
    output_path = generate_metasheet('temp', datadir, 'credentials.json', sheet_name, 'COGUK_submission_status', library_type, plate_list, run_name=run_name)
    if output_path:
        await ctx.send(f'Here is your upload sheet for {library_type} {plate_list}\n', file=discord.File(output_path))
        shutil.copy(output_path, output_path +'.attachment')
        await ctx.send(file=discord.File(output_path +'.attachment'))
        await ctx.send('You can submit this to http://metadata.cog-uk.io')
    else:
        await ctx.send(f"```\nERROR Generating sheet\n```\n")

@bot.command(pass_context=True, brief="Creates sample sheet for uploading existing samples", help="Creates a sample sheet read for upload through COG interactive submission" )
async def update_samplesheet(ctx, library_type, plate_list):
    if library_type.upper() in ['REACT']:
        sheet_name = 'SARSCOV2-REACT-Metadata'
    else: 
        sheet_name = 'SARCOV2-Metadata'
    await ctx.send("```\nReading sample sheets...\n```\n")
    datadir = None
    output_path = generate_metasheet('temp', datadir, 'credentials.json', sheet_name, 'COGUK_submission_status', library_type, plate_list, sample_only=True)
    if output_path:
        await ctx.send(f'Here is your upload sheet for {library_type} {plate_list}\n', file=discord.File(output_path))
        shutil.copy(output_path, output_path +'.attachment')
        await ctx.send(file=discord.File(output_path +'.attachment'))
        await ctx.send('You can submit this to http://metadata.cog-uk.io')
    else:
        await ctx.send(f"```\nERROR Generating sheet\n```\n")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        await ctx.send("**Invalid command. Try using** `$help` **to figure out commands!**")
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send('**Please pass in all requirements. See Help**')
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("**You dont have all the requirements or permissions for using this command :angry:**")
  
def main(args):
    config = load_config()
    chan_id = args.chanid
    log.setLevel(logging.INFO)
    if config.get('discord_token'):
        # bg = Back(bot)
        # bg_task = client.loop.create_task(bg_update_submission_table())
        bot.run(config.get('discord_token'))
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
