#!/usr/bin/env python3
import asyncio
import gc
import logging
import sys
import threading
import traceback
from datetime import datetime

from dask import distributed
from dask.distributed import Client
import dask.dataframe as dd
from assets.src.schemas import User
from assets.src import history, config, dt, preliminaries, user, determine_module
from assets.src.discord import discord
from assets.src.discord.services import bot, discord_token
from assets.src.database import api as database_api
import nextcord
from os import path, makedirs
import yaml
import uvicorn

"""LOAD CONFIGURATION"""
with open('config.yml', 'r') as file:
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
    print("- CHECK STARTED -")
    dt_start, timer_start = dt.timing()

    # CLUSTER DATA IS A LIST OF DICTIONARIES: STARTING WITH LAYER AS THE KEY
    process_msg = await discord.update_request_process_msg(process_msg, 1, None)
    # Reload Config
    pre_timer_start = dt.timing()[1]
    _configuration = await config.load()
    latest_tessellation_version = await preliminaries.latest_version_github(_configuration)
    all_cluster_data = await preliminaries.cluster_data(_configuration)
    pre_timer_stop = dt.timing()[1]
    print("- START PROCESS: GET PRELIMINARIES T =", pre_timer_stop-pre_timer_start, "-")
    await bot.wait_until_ready()
    await discord.init_process(bot, requester)
    data = await user.check(latest_tessellation_version, requester, all_cluster_data, dt_start, process_msg, _configuration)
    process_msg = await discord.update_request_process_msg(process_msg, 5, None)
    # Check if notification should be sent
    print("- NOTIFY: DETERMINE USING MODULE -")
    data = await determine_module.notify(data, _configuration)
    process_msg = await discord.update_request_process_msg(process_msg, 6, None)
    # If not request received through Discord channel
    print("- HISTORIC DATA: WRITE -")
    if process_msg is None:
        await history.write(data)
    # Write node id, ip, ports to subscriber list, then base code on id
    print("- DISCORD: SEND NOTIFICATION -")
    await discord.send(ctx, process_msg, bot, data, _configuration)
    await discord.update_request_process_msg(process_msg, 7, None)
    await asyncio.sleep(3)
    gc.collect()
    dt_stop, timer_stop = dt.timing()
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
    print(times)
    while True:
        print(datetime.time(datetime.utcnow()).strftime("%H:%M:%S"))
        if datetime.time(datetime.utcnow()).strftime("%H:%M:%S") in times:
            try:
                await main(None, None, None, _configuration)
            except Exception as e:
                traceback.print_exc()
                exit(1)

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
        # Clean data
        user_data = await User.discord(_configuration, "subscribe", str(ctx.message.author), int(ctx.message.author.id), *args)
        await user.write_db(user_data)
        await dask_client.close()


@bot.command()
async def u(ctx, *args):
    """This function treats a Discord message (context) as a line of arguments and attempts to unsubscribe the user"""

    pass


def run_uvicorn():
    uvicorn.run(database_api, host="127.0.0.1", port=8000)


if __name__ == "__main__":

    bot.loop.create_task(loop())

    # Create a thread for running uvicorn
    uvicorn_thread = threading.Thread(target=run_uvicorn)
    uvicorn_thread.start()
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
