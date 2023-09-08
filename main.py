#!/usr/bin/env python3.11
import asyncio
import gc
import logging
import threading
import traceback
from datetime import datetime

from assets.src import history, dt, preliminaries, user, determine_module, exception, api
from assets.src.discord import discord
from assets.src.discord.services import bot, discord_token

import nextcord
import yaml

"""LOAD CONFIGURATION"""
with open('config.yml', 'r') as file:
    _configuration = yaml.safe_load(file)

"""DEFINE LOGGING LEVEL AND LOCATION"""
logging.basicConfig(filename=_configuration["file settings"]["locations"]["log"], filemode='w',
                    format='[%(asctime)s] %(name)s - %(levelname)s - %(message)s', level=logging.ERROR)

version_manager = preliminaries.VersionManager(_configuration)

"""DISCORD COMMANDS"""

bot.load_extension('assets.src.discord.commands')
@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    ctx = await bot.get_context(message)
    if ctx.valid:
        logging.getLogger(__name__).info(
            f"main.py - Command received from {ctx.message.author} in {ctx.message.channel}")
        await bot.process_commands(message)
    else:
        if ctx.message.channel.id in (977357753947402281, 974431346850140204, 1030007676257710080, 1134396471639277648):
            # IGNORE INTERPRETING MESSAGES IN THESE CHANNELS AS COMMANDS
            logging.getLogger(__name__).info(
                f"main.py - Received a command in an non-command channel")
        elif ctx.message.channel.id == 1136386732628115636:
            logging.getLogger(__name__).info(
                f"main.py - Received a message in the verify channel")
            await discord.delete_message(ctx)
        else:
            logging.getLogger(__name__).info(
                f"main.py - Received an unknown command from {ctx.message.author} in {ctx.message.channel}")
            await message.add_reaction("\U0000274C")
            if not isinstance(ctx.message.channel, nextcord.DMChannel):
                await message.delete(delay=3)
            embed = await exception.command_error(ctx, bot)
            await ctx.message.author.send(embed=embed)
    return


@bot.event
async def on_ready():
    """Prints a message to the logs when connection to Discord is established (bot is running)"""
    logging.getLogger(__name__).info(f"main.py - Discord connection established")



"""MAIN LOOP"""
async def main(ctx, process_msg, requester, cluster_name, layer, _configuration) -> None:
    if requester is None:
        logging.getLogger(__name__).info(f"main.py - Automatic {cluster_name, layer} check initiated")
    else:
        logging.getLogger(__name__).info(f"main.py - Request from {requester} initiated")
    # GET GITHUB VERSION HERE
    dt_start, timer_start = dt.timing()
    process_msg = await discord.update_request_process_msg(process_msg, 1, None)
    cluster_data = await preliminaries.supported_clusters(cluster_name, layer, _configuration)
    ids = await api.get_user_ids(layer, requester, _configuration)

    await bot.wait_until_ready()
    data = await user.process_node_data_per_user(cluster_name, ids, requester, cluster_data, process_msg, version_manager, _configuration)
    process_msg = await discord.update_request_process_msg(process_msg, 5, None)
    data = await determine_module.notify(data, _configuration)
    process_msg = await discord.update_request_process_msg(process_msg, 6, None)
    if not process_msg:
        await history.write(data)
    await discord.send(ctx, process_msg, bot, data, _configuration)
    await discord.update_request_process_msg(process_msg, 7, None)
    dt_stop, timer_stop = dt.timing()
    if requester is None:
        logging.getLogger(__name__).info(
            f"main.py - Automatic {cluster_name, layer} check completed in {round(timer_stop - timer_start, 2)} seconds")
    else:
        logging.getLogger(__name__).info(
            f"main.py - Request from {requester} completed in {round(timer_stop - timer_start, 2)} seconds")


async def loop():
    async def loop_per_cluster_and_layer(cluster_name, layer):
        times = preliminaries.generate_runtimes(_configuration)
        logging.getLogger(__name__).info(f"main.py - {cluster_name, layer} runtime schedule:\n\t{times}")
        while True:
            if datetime.time(datetime.utcnow()).strftime("%H:%M:%S") in times:
                try:
                    await main(None, None, None, cluster_name, layer, _configuration)
                    await asyncio.sleep(3)
                    gc.collect()
                except Exception as e:
                    logging.getLogger(__name__).info(f"main.py - {cluster_name, layer} traceback:\n\t{traceback.print_exc()}")
                    break
            await asyncio.sleep(1)
        await loop_per_cluster_and_layer(cluster_name, layer)

    tasks = []
    for cluster_name in _configuration["modules"].keys():
        for layer in _configuration["modules"][cluster_name].keys():
            tasks.append(asyncio.create_task(loop_per_cluster_and_layer(cluster_name, layer)))
    for task in tasks:
        await task


if __name__ == "__main__":
    bot.loop.create_task(loop())

    # Create a thread for running uvicorn
    uvicorn_thread = threading.Thread(target=preliminaries.run_uvicorn)
    get_tessellation_version_thread = threading.Thread(target=version_manager.update_version)
    get_tessellation_version_thread.daemon = True
    get_tessellation_version_thread.start()
    uvicorn_thread.start()
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
