#!/usr/bin/env python3
import asyncio
import gc
import importlib.util
import logging
import sys
import time
from datetime import datetime

from aiofiles import os
from dask.distributed import Client
import distributed
from modules import extras, init, request, write
import nextcord
from nextcord.ext import commands
from os import getenv, path, makedirs
import yaml


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

    description = '''Bot by hgtp_Michael'''
    intents = nextcord.Intents.all()
    intents.members = True

    bot = commands.Bot(command_prefix='!', description=description, intents=intents)

    cluster = distributed.LocalCluster(asynchronous=True, n_workers=1, threads_per_worker=2, memory_limit='4GB',
                                       processes=True, silence_logs=logging.CRITICAL)

    async def main(requester, configuration) -> None:
        # CLUSTER DATA IS A LIST OF DICTIONARIES: STARTING WITH LAYER AS THE KEY
        configuration, all_supported_clusters_data,  latest_tessellation_version = await request.preliminary_data(configuration)
        await bot.wait_until_ready()
        async with Client(cluster) as dask_client:
            while not bot.is_closed():
                await dask_client.wait_for_workers(n_workers=1)
                logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - DASK CLIENT RUNNING")
                dt_start = datetime.utcnow()
                timer_start = time.perf_counter()
                await extras.set_active_presence(bot)
                data = []
                futures = await init.run(dask_client, requester, dt_start, latest_tessellation_version,  all_supported_clusters_data, configuration)
                for async_process in futures:
                    try:
                        data.append(await async_process)
                    except Exception as e:
                        logging.critical(repr(e.with_traceback(sys.exc_info())))
                        exit(1)
                # CREATE EMBEDS PER CLUSTER MODULE
                # all_data = sorted(all_data, key=lambda x: x["layer"])
                # all_data = sorted(all_data, key=lambda x: x["host"])
                # all_data = sorted(all_data, key=lambda x: x["contact"])
                futures.clear()
                # Check if notification should be sent
                for i, d in enumerate(data):
                    if await os.path.exists(
                            f"{configuration['file settings']['locations']['cluster modules']}/{d['clusterNames']}.py"):
                        spec = importlib.util.spec_from_file_location(f"{d['clusterNames']}.build_embed",
                                                                      f"{configuration['file settings']['locations']['cluster modules']}/{d['clusterNames']}.py")
                        module = importlib.util.module_from_spec(spec)
                        sys.modules[f"{d['clusterNames']}.build_embed"] = module
                        spec.loader.exec_module(module)
                        data[i] = module.mark_notify(d, configuration)
                # If not request received through Discord channel

                await write.history(dask_client, data, configuration)
                # Write node id, ip, ports to subscriber list, then base code on id
                await init.send(bot, data, configuration)
                await asyncio.sleep(0.8)
                gc.collect()
                timer_stop = time.perf_counter()
                print(timer_stop-timer_start)
                # exit(0)

    @bot.command()
    async def r(ctx, *arguments):
        if isinstance(ctx.channel, nextcord.DMChannel):
            await ctx.message.add_reaction('\U0001F4E5')
            requester = ctx.message.author
            await main(requester, configuration)
            await ctx.message.edit
        else:
            await ctx.message.add_reaction('\U0001F4E5')
            requester = ctx.message.author
            await main(requester, configuration)
            await ctx.message.delete(delay=3)


    @bot.event
    async def on_ready():
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CONNECTION TO DISCORD ESTABLISHED")

    bot.loop.create_task(main(None, configuration))
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
