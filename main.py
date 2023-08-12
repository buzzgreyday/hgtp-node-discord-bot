#!/usr/bin/env python3.11
import asyncio
import gc
import logging
import threading
import time
import traceback
from datetime import datetime

import requests as requests

from assets.src.schemas import User
from assets.src import history, config, dt, preliminaries, user, determine_module, exception
from assets.src.discord import discord
from assets.src.discord.services import bot, discord_token
from assets.src.database import api as database_api, api
import nextcord
import yaml
import uvicorn

"""LOAD CONFIGURATION"""
with open('config.yml', 'r') as file:
    _configuration = yaml.safe_load(file)

"""DEFINE LOGGING LEVEL AND LOCATION"""
logging.basicConfig(filename=_configuration["file settings"]["locations"]["log"], filemode='w',
                    format='[%(asctime)s] %(name)s - %(levelname)s - %(message)s', level=logging.INFO)


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
                f"main.py - Received a command in an non-command channel")
            pass
        elif ctx.message.channel.id == 1136386732628115636:
            logging.getLogger(__name__).info(
                f"main.py - Received a message in the verify channel")
            try:
                await ctx.message.author.send(f":hammer: ***\*banging on pipes***\*\n"
                                              f"> Return to the {ctx.channel.mention} channel")
                await discord.track_reactions(ctx, bot)
            except nextcord.Forbidden:
                await discord.verification_denied(ctx)
                logging.getLogger(__name__).info(f"discord.py - Verification of {ctx.message.author} denied")
            except asyncio.TimeoutError:
                logging.getLogger(__name__).info(f"discord.py - Verification of {ctx.message.author} denied: timed out")
                await ctx.message.delete()

        else:
            logging.getLogger(__name__).info(
                f"main.py - Received an unknown command from {ctx.message.author} in {ctx.message.channel}")
            await message.add_reaction("\U0000274C")
            if not isinstance(ctx.message.channel, nextcord.DMChannel):
                await message.delete(delay=3)
            embed = await exception.command_error(ctx, bot)
            await ctx.message.author.send(embed=embed)


@bot.command()
async def r(ctx):
    process_msg = await discord.send_request_process_msg(ctx)
    if process_msg:
        requester = await discord.get_requester(ctx)
        if not isinstance(ctx.channel, nextcord.DMChannel):
            await ctx.message.delete(delay=3)
        guild, member, role = await discord.return_guild_member_role(bot, ctx)
        if role:
            for name in _configuration["modules"].keys():
                for layer in _configuration["modules"][name].keys():
                    await main(ctx, process_msg, requester, name, layer, _configuration)
        else:
            logging.getLogger(__name__).info(
                f"discord.py - User {ctx.message.author} does not have the appropriate role")
            await discord.messages.subscriber_role_deny_request(process_msg)
    else:
        if not isinstance(ctx.channel, nextcord.DMChannel):
            await ctx.message.delete(delay=3)
        logging.getLogger(__name__).info(f"discord.py - User {ctx.message.author} does not allow DMs")


@bot.event
async def on_ready():
    """Prints a message to the logs when connection to Discord is established (bot is running)"""
    logging.getLogger(__name__).info(f"main.py - Discord connection established")


@bot.command()
async def s(ctx, *args):
    """This function treats a Discord message (context) as a line of arguments and attempts to create a new user subscription"""
    logging.getLogger(__name__).info(f"main.py - Subscription request received from {ctx.message.author}: {args}")
    process_msg = await discord.send_subscription_process_msg(ctx)
    valid_user_data, invalid_user_data = await User.discord(_configuration, process_msg, "subscribe",
                                                            str(ctx.message.author), int(ctx.message.author.id), *args)
    if valid_user_data:
        process_msg = await discord.update_subscription_process_msg(process_msg, 3, None)
        await user.write_db(valid_user_data)
        guild, member, role = await discord.return_guild_member_role(bot, ctx)
        await member.add_roles(role)
        await discord.update_subscription_process_msg(process_msg, 4, invalid_user_data)
        logging.getLogger(__name__).info(
            f"main.py - Subscription successful for {ctx.message.author}: {valid_user_data}\n\tDenied for: {invalid_user_data}")
    else:
        await discord.deny_subscription(process_msg)
        logging.getLogger(__name__).info(f"main.py - Subscription denied for {ctx.message.author}: {args}")

    if not isinstance(ctx.channel, nextcord.DMChannel):
        await ctx.message.delete(delay=3)


@bot.command()
async def u(ctx, *args):
    """This function treats a Discord message (context) as a line of arguments and attempts to unsubscribe the user"""
    logging.getLogger(__name__).info(f"main.py - Unubscription request received from {ctx.message.author}: {args}")


"""THREADS"""
def get_version_from_github():
    global latest_tessellation_version
    latest_tessellation_version = "0"
    i = 0
    while True:
        i += 1
        sleep = 3 ** i
        res = requests.get(f"{_configuration['tessellation']['github']['url']}/"
                           f"{_configuration['tessellation']['github']['releases']['latest']}")
        if res:
            latest_tessellation_version = res.json()["tag_name"][1:]
            time.sleep(30)
        else:
            logging.getLogger(__name__).warning(f"preliminaries.py - {_configuration['tessellation']['github']['url']}/{_configuration['tessellation']['github']['releases']['latest']} not reachable; forcing retry in {sleep} seconds")
            time.sleep(sleep)


def run_uvicorn():
    host = "127.0.0.1"
    port = 8000
    log_level = 'debug'
    logging.getLogger(__name__).info(f"main.py - Uvicorn running on {host}:{port}")
    uvicorn.run(database_api, host=host, port=port, log_level=log_level, log_config=f'assets/data/logs/bot/uvicorn.ini')


"""MAIN LOOP"""
async def main(ctx, process_msg, requester, name, layer, _configuration) -> None:
    if requester is None:
        logging.getLogger(__name__).info(f"main.py - Automatic {name, layer} check initiated")
    else:
        logging.getLogger(__name__).info(f"main.py - Request from {requester} initiated")
    dt_start, timer_start = dt.timing()
    process_msg = await discord.update_request_process_msg(process_msg, 1, None)
    _configuration = await config.load()
    cluster_data = await preliminaries.supported_clusters(name, layer, _configuration)
    ids = await api.get_user_ids(layer, requester, _configuration)
    await bot.wait_until_ready()
    data = await user.process_node_data_per_user(latest_tessellation_version, name, ids, requester, cluster_data, process_msg, _configuration)
    process_msg = await discord.update_request_process_msg(process_msg, 5, None)
    data = await determine_module.notify(data, _configuration)
    process_msg = await discord.update_request_process_msg(process_msg, 6, None)
    if not process_msg:
        await history.write(data)
    await discord.send(ctx, process_msg, bot, data, _configuration)
    await discord.update_request_process_msg(process_msg, 7, None)
    await asyncio.sleep(3)
    gc.collect()
    dt_stop, timer_stop = dt.timing()
    if requester is None:
        logging.getLogger(__name__).info(
            f"main.py - Automatic {name, layer} check completed in {round(timer_stop - timer_start, 2)} seconds")
    else:
        logging.getLogger(__name__).info(
            f"main.py - Request from {requester} completed in {round(timer_stop - timer_start, 2)} seconds")


async def loop():
    async def loop_de_loop(name, layer):
        times = preliminaries.generate_runtimes(_configuration)
        logging.getLogger(__name__).info(f"main.py - {name, layer} runtime schedule:\n\t{times}")
        while True:
            if datetime.time(datetime.utcnow()).strftime("%H:%M:%S") in times:
                try:
                    await main(None, None, None, name, layer, _configuration)
                except Exception as e:
                    logging.getLogger(__name__).info(f"main.py - {name, layer} traceback:\n\t{traceback.print_exc()}")
                    break
            await asyncio.sleep(1)
        await loop_de_loop(name, layer)

    tasks = []
    for name in _configuration["modules"].keys():
        for layer in _configuration["modules"][name].keys():
            tasks.append(asyncio.create_task(loop_de_loop(name, layer)))
    for task in tasks:
        await task


if __name__ == "__main__":
    bot.loop.create_task(loop())

    # Create a thread for running uvicorn
    uvicorn_thread = threading.Thread(target=run_uvicorn)
    get_tessellation_version_thread = threading.Thread(target=get_version_from_github)
    get_tessellation_version_thread.start()
    uvicorn_thread.start()
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
