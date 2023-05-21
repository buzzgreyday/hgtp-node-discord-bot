#!/usr/bin/env python3
import asyncio
import gc
import importlib.util
import logging
import re
import sys
import time
from datetime import datetime
from typing import Tuple, Any, List

from aiofiles import os
from dask.distributed import Client
import dask.dataframe as dd
import distributed
from modules import init, request, write, determine_module, date_and_time, read, subscription
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
    futures = []

    # CLUSTER DATA IS A LIST OF DICTIONARIES: STARTING WITH LAYER AS THE KEY
    process_msg = await discord.update_request_process_msg(process_msg, 1, None)
    configuration, all_supported_clusters_data, latest_tessellation_version = await request.preliminary_data(
        configuration)
    await bot.wait_until_ready()
    async with Client(cluster) as dask_client:
        await dask_client.wait_for_workers(n_workers=1)
        dt_start, timer_start = date_and_time.timing()
        await discord.init_process(bot, requester)
        history_dataframe = await read.history(configuration)
        subscriber_dataframe = await read.subscribers(configuration)
        for node_id in await subscription.locate_ids(dask_client, requester, subscriber_dataframe):
            subscriber = await subscription.locate_node(dask_client, subscriber_dataframe, node_id)
            for layer in list(set(subscriber["layer"])):
                for port in subscriber.public_port[subscriber.layer == layer]:
                    futures.append(asyncio.create_task(init.check(dask_client, bot, process_msg, requester, subscriber, port, layer, latest_tessellation_version, history_dataframe, all_supported_clusters_data, dt_start, configuration)))
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
    await discord.send(ctx, process_msg, bot, data, configuration)
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
async def s(ctx, *args):
    ipRegex = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"

    def slice_args_per_ip(args):
        sliced_args = []
        ips = list(set(filter(lambda ip: re.match(ipRegex, ip), args)))
        ip_idx = list(set(map(lambda ip: args.index(ip), ips)))
        for i in range(0, len(ip_idx)):
            if i + 1 < len(ip_idx):
                arg = args[ip_idx[i]: ip_idx[i + 1]]
                sliced_args.append(arg)
            else:
                arg = args[ip_idx[i]:]
                sliced_args.append(arg)
        return sliced_args

    def clean_args(arg) -> tuple[str, list[int], list[int]]:
        ip = None
        public_zero_ports = []
        public_one_ports = []
        for i, val in enumerate(arg):
            if re.match(ipRegex, val):
                ip = val
            elif val in ("z", "zero"):
                for port in arg[i + 1:]:
                    if port.isdigit():
                        public_zero_ports.append(port)
                    else:
                        break
            elif val in ("o", "one"):
                for port in arg[i + 1:]:
                    if port.isdigit():
                        public_one_ports.append(port)
                    else:
                        break
        return ip, public_zero_ports, public_one_ports

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
                        "layer": 0 if tpl[3] in ("z", "zero", "zeros") else 1 if tpl[3] in ("o", "one", "ones") else None,
                        "public_port": tpl[1],
                        "subscribed": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        }

    async with Client(cluster) as dask_client:
        await dask_client.wait_for_workers(n_workers=1)
        args = slice_args_per_ip(args)
        args = list(map(lambda arg: clean_args(arg), args))
        # Check which subscriptions exists and add to a list
        # Check which ip and ports are confirmed valid nodes ad add to list
        # Add non-confirmed nodes to list

        print(args)
        # await write.subscriber(dask_client, list_of_subs, configuration)


if __name__ == "__main__":
    bot.loop.create_task(loop())
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
