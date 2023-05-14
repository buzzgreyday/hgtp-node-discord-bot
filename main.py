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
from modules import init, request, write
from modules.discord import discord
import nextcord
from nextcord.ext import commands, tasks
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


def generate_runtimes() -> list:
    from datetime import datetime, timedelta

    start = datetime.strptime(configuration['general']['loop_init_time'], "%H:%M:%S")
    end = (datetime.strptime(configuration['general']['loop_init_time'], "%H:%M:%S") + timedelta(hours=24))
    return [(start + timedelta(hours=(configuration['general']['loop_interval_minutes']) * i / 60)).strftime("%H:%M:%S") for i in
            range(int((end - start).total_seconds() / 60.0 / (configuration['general']['loop_interval_minutes'])))]


if __name__ == "__main__":

    description = '''Bot by hgtp_Michael'''
    intents = nextcord.Intents.all()
    intents.members = True
    times = generate_runtimes()

    bot = commands.Bot(command_prefix='!', description=description, intents=intents)

    cluster = distributed.LocalCluster(asynchronous=True, n_workers=1, threads_per_worker=2, memory_limit='4GB',
                                       processes=True, silence_logs=logging.CRITICAL)

    async def main(ctx, process_msg, requester, configuration) -> None:
        data = []
        # CLUSTER DATA IS A LIST OF DICTIONARIES: STARTING WITH LAYER AS THE KEY
        process_msg = await discord.update_proces_msg(process_msg, 1, None)
        configuration, all_supported_clusters_data,  latest_tessellation_version = await request.preliminary_data(configuration)
        await bot.wait_until_ready()
        async with Client(cluster) as dask_client:
            await dask_client.wait_for_workers(n_workers=1)
            logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - DASK CLIENT RUNNING")
            dt_start = datetime.utcnow()
            timer_start = time.perf_counter()
            await discord.init_process(bot, requester)
            futures = await init.process(dask_client, process_msg, requester, dt_start, latest_tessellation_version,  all_supported_clusters_data, configuration)
            for async_process in futures:
                try:
                    d, process_msg = await async_process
                    data.append(d)
                except Exception as e:
                    logging.critical(repr(e.with_traceback(sys.exc_info())))
                    exit(1)
            # CREATE EMBEDS PER CLUSTER MODULE
            # all_data = sorted(all_data, key=lambda x: x["layer"])
            # all_data = sorted(all_data, key=lambda x: x["host"])
            # all_data = sorted(all_data, key=lambda x: x["contact"])
            futures.clear()
            process_msg = await discord.update_proces_msg(process_msg, 5, None)
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
            process_msg = await discord.update_proces_msg(process_msg, 6, None)

            # If not request received through Discord channel
            if process_msg is None:
                await write.history(dask_client, data, configuration)
        # Write node id, ip, ports to subscriber list, then base code on id
        await init.send(ctx, process_msg, bot, data, configuration)
        await discord.update_proces_msg(process_msg, 7, None)
        await asyncio.sleep(3)
        gc.collect()
        timer_stop = time.perf_counter()
        print(timer_stop-timer_start)


    async def command_error(ctx, bot):

        embed = nextcord.Embed(title="Command not found".upper(),
                               color=nextcord.Color.orange())
        embed.insert_field_at(index=0, name=f"\U00002328` {ctx.message.content}`",
                              value=f"`ⓘ Please make sure you did enter a proper command`",
                              inline=False)
        embed.set_author(name=ctx.message.author,
                         icon_url=bot.get_user(
                             ctx.message.author.id).display_avatar.url)
        return embed


    @bot.event
    async def on_message(message):
        if message.author == bot.user:
            return
        ctx = await bot.get_context(message)
        if ctx.valid:
            print("Command received")
            await bot.process_commands(message)
        else:
            if ctx.message.channel.id in (977357753947402281, 974431346850140204, 1030007676257710080):
                # IGNORE INTERPRETING MESSAGES IN THESE CHANNELS AS COMMANDS
                print("Command in non-command channel")
                pass
            else:
                await message.add_reaction("\U0000274C")
                print("Command in command channel but not a command")
                if not isinstance(ctx.message.channel, nextcord.DMChannel):
                    await message.delete(delay=None)
                embed = await command_error(ctx, bot)
                await ctx.message.author.send(embed=embed)


    @bot.command()
    async def r(ctx, *arguments):
        process_msg = await discord.send_process_msg(ctx)
        requester = await discord.get_requester(ctx)
        if isinstance(ctx.channel, nextcord.DMChannel):
            await ctx.message.delete(delay=3)
        await main(ctx, process_msg, requester, configuration)


    async def loop():
        while True:
            if datetime.time(datetime.utcnow()).strftime("%H:%M:%S") in times:
                await main(None, None, None, configuration)
            else:
                print(datetime.time(datetime.utcnow()).strftime("%H:%M:%S"), "SKIPPING...")
            await asyncio.sleep(1)


    @bot.event
    async def on_ready():
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CONNECTION TO DISCORD ESTABLISHED")
    bot.loop.create_task(loop())
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
