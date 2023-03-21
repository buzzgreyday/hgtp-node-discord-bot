import asyncio
import logging
import sys
import time
from datetime import datetime
from dask.distributed import Client
import distributed
from functions import extras, async_processes
import nextcord
from nextcord.ext import commands
from os import getenv, path, makedirs
import yaml

# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

"""LOAD DISCORD SERVER TOKEN FROM ENVIRONMENT"""
discord_token = getenv("HGTP_SPIDR_DISCORD_TOKEN")

"""LOAD CONFIGURATION"""
with open('data/config.yml', 'r') as file:
    configuration = yaml.safe_load(file)

"""CREATE NON-EXISTENT FOLDER STRUCTURE"""
if not path.exists(configuration["file settings"]["locations"]["log"]):
    makedirs(configuration["file settings"]["locations"]["log"])

"""DEFINE LOGGING LEVEL AND LOCATION"""
logging.basicConfig(filename=configuration["file settings"]["locations"]["log"], filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

if __name__ == "__main__":

    description = '''Keeps track on your Constellation nodes'''
    intents = nextcord.Intents.all()
    intents.members = True
    bot = commands.Bot(command_prefix='!', description=description, intents=intents)

    cluster = distributed.LocalCluster(asynchronous=True, n_workers=1, threads_per_worker=2, memory_limit='4GB',
                                       processes=True, silence_logs=logging.CRITICAL)

    async def main(configuration) -> None:
        # CLUSTER DATA IS A LIST OF DICTIONARIES: STARTING WITH LAYER AS THE KEY
        configuration, all_supported_clusters_data,  validator_mainnet_data, validator_testnet_data, latest_tessellation_version = await async_processes.preliminary_data(configuration)
        await bot.wait_until_ready()
        async with Client(cluster) as dask_client:
            while not bot.is_closed():
                await dask_client.wait_for_workers(n_workers=1)
                logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - DASK CLIENT RUNNING")
                timer_start = time.perf_counter()
                await extras.set_active_presence(bot)
                futures = await async_processes.init(dask_client, latest_tessellation_version,  validator_mainnet_data, validator_testnet_data, all_supported_clusters_data, configuration)
                for async_process in futures:
                    try:
                        node_data = await async_process
                    except Exception as e:
                        logging.critical(repr(e.with_traceback(sys.exc_info())))
                        exit(1)
                timer_stop = time.perf_counter()
                print(timer_stop-timer_start)
                exit(0)


    @bot.event
    async def on_ready():
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CONNECTION TO DISCORD ESTABLISHED")


    bot.loop.create_task(main(configuration))
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
