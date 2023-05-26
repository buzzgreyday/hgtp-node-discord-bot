#!/usr/bin/env python3
import asyncio
import gc
import logging
import sys
from datetime import datetime

import pandas as pd
from dask import distributed
from dask.distributed import Client
import dask.dataframe as dd
from modules import determine_module, dt, subscription, history, preliminaries, node, config
from modules.discord import discord
from modules.discord.services import bot, discord_token
import nextcord
from os import path, makedirs
import yaml

"""LOAD CONFIGURATION"""
with open('data/config_new.yml', 'r') as file:
    _configuration = yaml.safe_load(file)

"""CREATE NON-EXISTENT FOLDER STRUCTURE"""
if not path.exists(_configuration["file settings"]["locations"]["log"]):
    makedirs(_configuration["file settings"]["locations"]["log"])

"""DEFINE LOGGING LEVEL AND LOCATION"""
logging.basicConfig(filename=_configuration["file settings"]["locations"]["log"], filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

cluster = distributed.LocalCluster(asynchronous=True, n_workers=1, threads_per_worker=2, memory_limit='4GB',
                                   processes=True, silence_logs=logging.CRITICAL)


def generate_runtimes() -> list:
    from datetime import datetime, timedelta

    start = datetime.strptime(_configuration['general']['loop_init_time'], "%H:%M:%S")
    end = (datetime.strptime(_configuration['general']['loop_init_time'], "%H:%M:%S") + timedelta(hours=24))
    return [(start + timedelta(hours=(_configuration['general']['loop_interval_minutes']) * i / 60)).strftime("%H:%M:%S")
            for i in
            range(int((end - start).total_seconds() / 60.0 / (_configuration['general']['loop_interval_minutes'])))]


async def main(ctx, process_msg, requester, _configuration) -> None:
    dt_start, timer_start = dt.timing()
    data = []
    futures = []

    # CLUSTER DATA IS A LIST OF DICTIONARIES: STARTING WITH LAYER AS THE KEY
    process_msg = await discord.update_request_process_msg(process_msg, 1, None)
    # Reload Config
    pre_timer_start = dt.timing()[1]
    _configuration = await config.load()
    latest_tessellation_version = await preliminaries.latest_version_github(_configuration)
    all_cluster_data = await preliminaries.cluster_data(_configuration)
    pre_timer_stop = dt.timing()[1]
    print("PRELIMINARIES TIME:", pre_timer_stop-pre_timer_start)
    await bot.wait_until_ready()
    async with Client(cluster) as dask_client:
        await dask_client.wait_for_workers(n_workers=1)
        await discord.init_process(bot, requester)
        history_dataframe = await history.read(_configuration)
        subscriber_dataframe = await subscription.read(_configuration)
        data = await subscription.check(dask_client, latest_tessellation_version, requester, subscriber_dataframe, history_dataframe, all_cluster_data, dt_start, process_msg, _configuration)
        process_msg = await discord.update_request_process_msg(process_msg, 5, None)
        # Check if notification should be sent
        data = await determine_module.notify(data, _configuration)
        process_msg = await discord.update_request_process_msg(process_msg, 6, None)
        # If not request received through Discord channel
        if process_msg is None:
            await history.write(dask_client, data, _configuration)
    # Write node id, ip, ports to subscriber list, then base code on id
    await discord.send(ctx, process_msg, bot, data, _configuration)
    await discord.update_request_process_msg(process_msg, 7, None)
    await asyncio.sleep(3)
    gc.collect()
    dt_stop, timer_stop = dt.timing()
    await dask_client.close()
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
    await main(ctx, process_msg, requester, _configuration)


async def loop():
    times = generate_runtimes()
    while True:
        if datetime.time(datetime.utcnow()).strftime("%H:%M:%S") in times:
            await main(None, None, None, _configuration)
        await asyncio.sleep(1)


@bot.event
async def on_ready():
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CONNECTION TO DISCORD ESTABLISHED")


@bot.command()
async def s(ctx, *args):
    subscriptions = []
    existing_subscriptions = []
    invalid_subscriptions = []

    async with Client(cluster) as dask_client:
        await dask_client.wait_for_workers(n_workers=1)
        subscriber_dataframe = await subscription.read(_configuration)  # Load the dataframe using the read function
        for arg in list(map(lambda arg: subscription.clean_args(arg), subscription.slice_args_per_ip(args))):
            # for each possible layer
            for L in (0, 1):
                # check for each layer port subscription
                for port in arg[L]:
                    print(arg[2], port, L)
                    filtered_df = await dask_client.compute(subscriber_dataframe[(subscriber_dataframe["ip"] == arg[2]) & (subscriber_dataframe["public_port"] == port) & (subscriber_dataframe["layer"] == L)])
                    print(filtered_df)
                    if len(filtered_df) == 0:
                        print(f"NO SUBSCRIPTION FOUND FOR IP {arg[2]} PORT {port}")
                        subscriptions.append([L, arg[2], port])
                    else:
                        print(f"SUBSCRIPTION FOUND FOR IP {arg[2]} PORT {port}")
                        existing_subscriptions.append([L, arg[2], port])

        for idx, sub_data in enumerate(subscriptions):
            identity = await subscription.validate_subscriber(sub_data[1], sub_data[2], _configuration)
            if identity is None:
                invalid_subscriptions.append(sub_data.append(identity))
                subscriptions.pop(idx)
            else:
                sub_data.append(identity)

        print(subscriptions, invalid_subscriptions, existing_subscriptions)
        data = []
        for sub_data in subscriptions:
            data.append({"public_port": int(sub_data[2]), "layer": int(sub_data[0]), "ip": str(sub_data[1]), "id": str(sub_data[3]), "name": str(ctx.message.author), "contact": int(ctx.message.author.id)})
        new_dataframe = dd.from_pandas(pd.DataFrame(data), npartitions=1)
        if len(await dask_client.compute(subscriber_dataframe)) == 0:
            subscriber_dataframe = new_dataframe
        else:
            subscriber_dataframe = subscriber_dataframe.append(new_dataframe)
        print(await dask_client.compute(subscriber_dataframe.reset_index(drop=True)))

        await subscription.write(dask_client, subscriber_dataframe.reset_index(drop=True), _configuration)
        await dask_client.close()


if __name__ == "__main__":
    bot.loop.create_task(loop())
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
