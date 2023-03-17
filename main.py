import logging
import time
from datetime import datetime
from dask.distributed import Client
import distributed
from functions import aesthetics, process
import nextcord
from nextcord.ext import commands
from os import getenv, path, makedirs
import yaml

"""LOAD DISCORD SERVER TOKEN FROM ENVIRONMENT"""
discord_token = getenv("HGTP_SPIDR_DISCORD_TOKEN")

"""LOAD CONFIGURATION"""
with open('data/configuration.yaml', 'r') as file:
    configuration = yaml.safe_load(file)

"""CREATE NON-EXISTENT FOLDER STRUCTURE"""
if not path.exists(configuration["file locations"]["log"]):
    makedirs(configuration["file locations"]["log"])

"""DEFINE LOGGING LEVEL AND LOCATION"""
logging.basicConfig(filename=configuration["file locations"]["log"], filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

if __name__ == "__main__":

    description = '''Keeps track on your Constellation nodes'''
    intents = nextcord.Intents.all()
    intents.members = True
    bot = commands.Bot(command_prefix='!', description=description, intents=intents)

    cluster = distributed.LocalCluster(asynchronous=True, n_workers=1, threads_per_worker=2, memory_limit='4GB',
                                       processes=True, silence_logs=logging.CRITICAL)

    async def main():
        list_of_cluster_snapshots_to_request = []
        list_of_existing_cluster_snapshots = []
        await bot.wait_until_ready()
        async with Client(cluster) as dask_client:
            while not bot.is_closed():
                await dask_client.wait_for_workers(n_workers=1)
                logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - DASK CLIENT RUNNING")
                timer_start = time.perf_counter()
                await aesthetics.set_active_presence(bot)
                futures = await process.init(dask_client, configuration)
                for _ in futures:
                    try:
                        node_data, cluster_data = await _
                        for k, v in node_data.items():
                            if (k == "clusterName") and (v in list(configuration["source ids"].keys())):
                                print(v)
                                list_of_cluster_snapshots_to_request.append(v)


                    # dictionary
                        print(node_data)
                    except Exception as e:
                        exit(1)
                    # list of dict (cluster_data)
                timer_stop = time.perf_counter()
                print(timer_stop-timer_start)
                exit(0)


    @bot.event
    async def on_ready():
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CONNECTION TO DISCORD ESTABLISHED")


    bot.loop.create_task(main())
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
