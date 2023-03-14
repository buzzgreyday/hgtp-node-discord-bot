import logging
import time
from datetime import datetime
from dask.distributed import Client
import distributed
from configuration import logging_dir
from functions import aesthetics, concurrent_tasking
import nextcord
from nextcord.ext import commands
from os import getenv, path, makedirs
discord_token = getenv("HGTP_SPIDR_DISCORD_TOKEN")
if not path.exists(logging_dir):
    makedirs(logging_dir)

logging.basicConfig(filename=f'{logging_dir}/bot.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

if __name__ == "__main__":

    description = '''Keeps track on your Constellation nodes'''
    intents = nextcord.Intents.all()
    intents.members = True
    bot = commands.Bot(command_prefix='!', description=description, intents=intents)

    cluster = distributed.LocalCluster(asynchronous=True, n_workers=1, threads_per_worker=2, memory_limit='4GB',
                                       processes=True, silence_logs=logging.CRITICAL)

    async def main():
        await bot.wait_until_ready()
        async with Client(cluster) as dask_client:
            while not bot.is_closed():
                await dask_client.wait_for_workers(n_workers=1)
                logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - DASK CLIENT RUNNING")
                timer_start = time.perf_counter()
                await aesthetics.set_active_presence(bot)
                futures = await concurrent_tasking.init(dask_client)
                for _ in futures:
                    node_data, cluster_data = await _
                    # dictionary
                    print(node_data)
                    # list of dict (cluster_data)
                timer_stop = time.perf_counter()
                print(timer_stop-timer_start)
                exit(0)


    @bot.event
    async def on_ready():
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CONNECTION TO DISCORD ESTABLISHED")


    bot.loop.create_task(main())
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
