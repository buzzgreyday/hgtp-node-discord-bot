#!/usr/bin/env python3
import asyncio
import gc
import importlib.util
import logging
import re
import sys
import time
from datetime import datetime

from aiofiles import os
from dask.distributed import Client
import distributed
from modules import init, request, write, determine_module, date_and_time
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
logging.basicConfig(filename=configuration["file settings"]["locations"]["log"], filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

description = '''Bot by hgtp_Michael'''
intents = nextcord.Intents.all()
intents.members = True

cluster = distributed.LocalCluster(asynchronous=True, n_workers=1, threads_per_worker=2, memory_limit='4GB',
                                   processes=True, silence_logs=logging.CRITICAL)

bot = commands.Bot(command_prefix='!', description=description, intents=intents)


def generate_runtimes() -> list:
    from datetime import datetime, timedelta

    start = datetime.strptime(configuration['general']['loop_init_time'], "%H:%M:%S")
    end = (datetime.strptime(configuration['general']['loop_init_time'], "%H:%M:%S") + timedelta(hours=24))
    return [(start + timedelta(hours=(configuration['general']['loop_interval_minutes']) * i / 60)).strftime("%H:%M:%S")
            for i in
            range(int((end - start).total_seconds() / 60.0 / (configuration['general']['loop_interval_minutes'])))]


async def main(ctx, process_msg, requester, configuration) -> None:
    data = []
    # CLUSTER DATA IS A LIST OF DICTIONARIES: STARTING WITH LAYER AS THE KEY
    process_msg = await discord.update_request_process_msg(process_msg, 1, None)
    configuration, all_supported_clusters_data, latest_tessellation_version = await request.preliminary_data(
        configuration)
    await bot.wait_until_ready()
    async with Client(cluster) as dask_client:
        await dask_client.wait_for_workers(n_workers=1)
        dt_start, timer_start = date_and_time.timing()
        await discord.init_process(bot, requester)
        futures = await init.process(dask_client, bot, process_msg, requester, dt_start, latest_tessellation_version,
                                     all_supported_clusters_data, configuration)
        for async_process in futures:
            try:
                d, process_msg = await async_process
                data.append(d)
            except Exception as e:
                logging.critical(repr(e.with_traceback(sys.exc_info())))
                exit(1)
        futures.clear()
        process_msg = await discord.update_request_process_msg(process_msg, 5, None)
        # Check if notification should be sent
        data = await determine_module.notify(data, configuration)
        process_msg = await discord.update_request_process_msg(process_msg, 6, None)
        # If not request received through Discord channel
        if process_msg is None:
            await write.history(dask_client, data, configuration)
    # Write node id, ip, ports to subscriber list, then base code on id
    await init.send(ctx, process_msg, bot, data, configuration)
    await discord.update_request_process_msg(process_msg, 7, None)
    await asyncio.sleep(3)
    gc.collect()
    dt_stop, timer_stop = date_and_time.timing()
    print(timer_stop - timer_start)


async def command_error(ctx, bot):
    embed = nextcord.Embed(title="Command not found".upper(),
                           color=nextcord.Color.orange())
    embed.add_field(name=f"\U00002328` {ctx.message.content}`",
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
                await message.delete(delay=3)
            embed = await command_error(ctx, bot)
            await ctx.message.author.send(embed=embed)


@bot.command()
async def r(ctx, *arguments):
    process_msg = await discord.send_request_process_msg(ctx)
    requester = await discord.get_requester(ctx)
    if isinstance(ctx.channel, nextcord.DMChannel):
        await ctx.message.delete(delay=3)
    await main(ctx, process_msg, requester, configuration)


async def loop():
    times = generate_runtimes()
    while True:
        if datetime.time(datetime.utcnow()).strftime("%H:%M:%S") in times:
            await main(None, None, None, configuration)
        await asyncio.sleep(1)


@bot.event
async def on_ready():
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CONNECTION TO DISCORD ESTABLISHED")


@bot.command()
async def s(ctx, *arguments):
    ipRegex = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"
    ips = list(set(filter(lambda ip: re.match(ipRegex, ip), arguments)))
    ip_idx = list(set(map(lambda ip: arguments.index(ip), ips)))
    list_of_subs = []

    async def slice_and_check_args(idx: int, ip: str, *args) -> tuple[list[tuple], list[tuple]]:
        valid = []
        not_valid = []
        sliced_args = arguments[idx:]
        for idx, arg in enumerate(map(lambda arg: arg in args, sliced_args)):
            if arg:
                for port in sliced_args[idx + 1:]:
                    if port.isdigit():
                        node_id = await validate_node(ip, port)
                        if node_id is not None:
                            valid.append((ip, port, node_id))
                        else:
                            not_valid.append((ip, port, None))
                    else:
                        break
                break
        return valid, not_valid

    async def validate_node(ip: str, port: str):
        print("Requesting:", f"http://{str(ip)}:{str(port)}/node/info")
        node_data = await request.safe(f"http://{ip}:{port}/{configuration['request']['url']['clusters']['url endings']['node info']}", configuration)
        return node_data["id"] if node_data is not None else None

    def return_valid_subscriber_dictionary(valid):
        for tpl in valid:
            if tpl[2] is not None:
                return {
                            "id": tpl[2],
                            "name": ctx.message.author.name,
                            "contact": ctx.message.author.id,
                            "ip": tpl[0],
                            "public_l0": tpl[1],
                            "public_l1": None,
                            "subscribed": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        }

    """async with Client(cluster) as dask_client:
        await dask_client.wait_for_workers(n_workers=1)"""
    for i, idx in enumerate(ip_idx):
        # There is "i" number of IP indexes associated with the subscription command. Choose the number "i" IP, indexed
        # at "idx" in command args
        valid_zero, not_valid_zero = await slice_and_check_args(idx, ips[i], "z", "zero", "zeros")
        list_of_subs.append(return_valid_subscriber_dictionary(valid_zero))
        valid_one, not_valid_one = await slice_and_check_args(idx, ips[i], "o", "one", "ones")
        list_of_subs.append(return_valid_subscriber_dictionary(valid_one))

        # if await os.path.exists(configuration['file settings']['locations']['subscriber data']):
        # await write.subscriber(dask_client, subscription, configuration)

    print(list_of_subs)


if __name__ == "__main__":
    bot.loop.create_task(loop())
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
