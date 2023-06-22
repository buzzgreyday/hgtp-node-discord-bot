#!/usr/bin/env python3
import asyncio
import gc
import logging
import threading
from datetime import datetime

import pandas as pd
from dask import distributed
from dask.distributed import Client
import dask.dataframe as dd
from assets.code.schemas import User
from assets.code import config, determine_module, history, user, dt, preliminaries
from assets.code.discord import discord
from assets.code.discord.services import bot, discord_token
from assets.code.database import api
import nextcord
from os import path, makedirs
import yaml
import uvicorn

"""LOAD CONFIGURATION"""
with open('config_new.yml', 'r') as file:
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
        subscriber_dataframe = await user.read(_configuration)
        data = await user.check(dask_client, latest_tessellation_version, requester, subscriber_dataframe, history_dataframe, all_cluster_data, dt_start, process_msg, _configuration)
        process_msg = await discord.update_request_process_msg(process_msg, 5, None)
        # Check if notification should be sent
        data = await determine_module.notify(data, _configuration)
        process_msg = await discord.update_request_process_msg(process_msg, 6, None)
        # If not request received through Discord channel
        if process_msg is None:
            await history.write(dask_client, history_dataframe, data, _configuration)
    # Write node id, ip, ports to subscriber list, then base code on id
    await discord.send(ctx, process_msg, bot, data, _configuration)
    await discord.update_request_process_msg(process_msg, 7, None)
    await asyncio.sleep(3)
    gc.collect()
    dt_stop, timer_stop = dt.timing()
    await dask_client.close()
    print("ALL PROCESSES RAN IN:", timer_stop - timer_start, "SECONDS")


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
    """Prints a message to the logs when connection to Discord is established (bot is running)"""
    logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CONNECTION TO DISCORD ESTABLISHED")


@bot.command()
async def s(ctx, *args):
    """This function treats a Discord message (context) as a line of arguments and attempts to create a new user subscription"""

    async with Client(cluster) as dask_client:
        await dask_client.wait_for_workers(n_workers=1)
        subscriber_dataframe = await user.read(_configuration)  # Load the dataframe using the read function
        # The class below creates a list of user objects. These should be subscibed.
        user_data = await User.discord(_configuration, "subscribe", str(ctx.message.author), int(ctx.message.author.id), *args)
        await user.write_db(user_data)
        # Check if data exists or ID is None schemas.UserCreate.validate
        new_dataframe = dd.from_pandas(pd.DataFrame(list(data.dict() for data in user_data)), npartitions=1)
        if len(await dask_client.compute(subscriber_dataframe)) == 0:
            subscriber_dataframe = new_dataframe
        else:
            subscriber_dataframe = subscriber_dataframe.append(new_dataframe)
        await user.write(dask_client, subscriber_dataframe.reset_index(drop=True), _configuration)

        await dask_client.close()


@bot.command()
async def u(ctx, *args):
    """This function treats a Discord message (context) as a line of arguments and attempts to unsubscribe the user"""

    async with Client(cluster) as dask_client:
        await dask_client.wait_for_workers(n_workers=1)
        subscriber_dataframe = await user.read(_configuration)  # Load the dataframe using the read function
        # The class below creates a list of user objects. These should be unsubscibed.
        user_data = await User.discord(_configuration, "unsubscribe", str(ctx.message.author), int(ctx.message.author.id), *args)
        subscriber_dataframe = await dask_client.compute(subscriber_dataframe)
        for dict_ in user_data:
            subscriber_dataframe = subscriber_dataframe.drop(subscriber_dataframe[(subscriber_dataframe.name == dict_.name) & (subscriber_dataframe.contact == dict_.contact) & (subscriber_dataframe.ip == dict_.ip) & (subscriber_dataframe.public_port == dict_.public_port) & (subscriber_dataframe.type == "discord")].index)
            subscriber_dataframe = await dd.from_pandas(subscriber_dataframe, npartitions=1)
        await user.write(dask_client, subscriber_dataframe, _configuration)
        await dask_client.close()


def run_uvicorn():
    uvicorn.run(api, host="127.0.0.1", port=8000)


if __name__ == "__main__":

    bot.loop.create_task(loop())

    # Create a thread for running uvicorn
    uvicorn_thread = threading.Thread(target=run_uvicorn)
    uvicorn_thread.start()
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
