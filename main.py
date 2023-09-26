#!/usr/bin/env python3.11
import asyncio
import gc
import logging
import threading
import traceback
from datetime import datetime

import nextcord
import yaml

from assets.src import preliminaries, exception, run_process, history
from assets.src.discord import discord
from assets.src.discord.services import bot, discord_token

"""LOAD CONFIGURATION"""
with open('config.yml', 'r') as file:
    _configuration = yaml.safe_load(file)

"""DEFINE LOGGING LEVEL AND LOCATION"""
logging.basicConfig(filename=_configuration["file settings"]["locations"]["log"], filemode='w',
                    format='[%(asctime)s] %(name)s - %(levelname)s - %(message)s', level=logging.ERROR)

version_manager = preliminaries.VersionManager(_configuration)

"""DISCORD COMMANDS"""

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
                f"main.py - Received a command in a non-command channel")
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

@bot.event
async def on_ready():
    """Prints a message to the logs when a connection to Discord is established (bot is running)"""
    logging.getLogger(__name__).info(f"main.py - Discord connection established")

"""MAIN LOOP"""


async def loop_per_cluster_and_layer(cluster_name, layer):
    try:
        data = await run_process.automatic_check(cluster_name, layer, _configuration)
    except Exception:
        logging.getLogger(__name__).error(f"main.py - error: {traceback.format_exc()}\n\tRestarting {cluster_name}, L{layer}")
        await discord.messages.send_traceback(bot, traceback.format_exc())
        await asyncio.sleep(3)
        await loop_per_cluster_and_layer(cluster_name, layer)
    else:
        await asyncio.sleep(3)
        gc.collect()
        return data

async def main_loop():
    data_queue = asyncio.Queue()
    tasks = []
    times = preliminaries.generate_runtimes(_configuration)
    logging.getLogger(__name__).info(f"main.py - runtime schedule:\n\t{times}")
    while True:
        if datetime.time(datetime.utcnow()).strftime("%H:%M:%S") in times:
            for cluster_name in _configuration["modules"].keys():
                for layer in _configuration["modules"][cluster_name].keys():
                    task = asyncio.create_task(loop_per_cluster_and_layer(cluster_name, layer))
                    tasks.append(task)
            for completed_task in asyncio.as_completed(tasks):
                data = await completed_task
                await data_queue.put(data)
            while not data_queue.empty():
                data = await data_queue.get()
                await history.write(data)


async def main():
    await bot.start(discord_token, reconnect=True)

if __name__ == "__main__":
    bot.load_extension('assets.src.discord.commands')

    loop = asyncio.get_event_loop()
    loop.create_task(main_loop())
    loop.create_task(main())

    # Create a thread for running uvicorn
    uvicorn_thread = threading.Thread(target=preliminaries.run_uvicorn)
    get_tessellation_version_thread = threading.Thread(target=version_manager.update_version)
    get_tessellation_version_thread.daemon = True
    get_tessellation_version_thread.start()
    uvicorn_thread.start()
    loop.run_forever()
