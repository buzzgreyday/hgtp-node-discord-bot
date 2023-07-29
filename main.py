#!/usr/bin/env python3
import asyncio
import gc
import logging
import threading
import traceback
from datetime import datetime

from assets.src.schemas import User
from assets.src import history, config, dt, preliminaries, user, determine_module
from assets.src.discord import discord
from assets.src.discord.services import bot, discord_token
from assets.src.database import api as database_api
import nextcord
import yaml
import uvicorn

"""LOAD CONFIGURATION"""
with open('config.yml', 'r') as file:
    _configuration = yaml.safe_load(file)

"""DEFINE LOGGING LEVEL AND LOCATION"""
logging.basicConfig(filename=_configuration["file settings"]["locations"]["log"], filemode='w',
                    format='[%(asctime)s] %(name)s - %(levelname)s - %(message)s', level=logging.INFO)


def generate_runtimes() -> list:
    from datetime import datetime, timedelta

    start = datetime.strptime(_configuration['general']['loop_init_time'], "%H:%M:%S")
    end = (datetime.strptime(_configuration['general']['loop_init_time'], "%H:%M:%S") + timedelta(hours=24))
    return [(start + timedelta(hours=(_configuration['general']['loop_interval_minutes']) * i / 60)).strftime("%H:%M:%S")
            for i in
            range(int((end - start).total_seconds() / 60.0 / (_configuration['general']['loop_interval_minutes'])))]


async def main(ctx, process_msg, requester, _configuration) -> None:
    if requester is None:
        logging.getLogger(__name__).info(f"main.py - Automatic check initiated")
    else:
        logging.getLogger(__name__).info(f"main.py - Request from {requester} initiated")
    dt_start, timer_start = dt.timing()
    process_msg = await discord.update_request_process_msg(process_msg, 1, None)
    _configuration = await config.load()
    latest_tessellation_version = await preliminaries.latest_version_github(_configuration)
    all_cluster_data = await preliminaries.cluster_data(_configuration)
    await bot.wait_until_ready()
    data = await user.check(latest_tessellation_version, requester, all_cluster_data, dt_start, process_msg, _configuration)
    process_msg = await discord.update_request_process_msg(process_msg, 5, None)
    data = await determine_module.notify(data, _configuration)
    process_msg = await discord.update_request_process_msg(process_msg, 6, None)
    if process_msg is None:
        await history.write(data)
    await discord.send(ctx, process_msg, bot, data, _configuration)
    await discord.update_request_process_msg(process_msg, 7, None)
    await asyncio.sleep(3)
    gc.collect()
    dt_stop, timer_stop = dt.timing()
    if requester is None:
        logging.getLogger(__name__).info(f"main.py - Automatic check completed in {round(timer_stop - timer_start, 2)} seconds")
    else:
        logging.getLogger(__name__).info(f"main.py - Request from {requester} completed in {round(timer_stop - timer_start, 2)} seconds")


async def command_error(ctx, bot):
    embed = nextcord.Embed(title="Not a valid command".upper(),
                           color=nextcord.Color.orange())
    embed.add_field(name=f"\U00002328` {ctx.message.content}`",
                    value=f"`ⓘ You didn't input a valid`",
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
        logging.getLogger(__name__).info(f"main.py - Command received from {ctx.message.author} in {ctx.message.channel}")
        await bot.process_commands(message)
    else:
        if ctx.message.channel.id in (977357753947402281, 974431346850140204, 1030007676257710080, 1134396471639277648):
            # IGNORE INTERPRETING MESSAGES IN THESE CHANNELS AS COMMANDS
            logging.getLogger(__name__).info(
                f"main.py - Received a command in an non-command channel")
            pass
        else:
            logging.getLogger(__name__).info(
                f"main.py - Received an unknown command from {ctx.message.author} in {ctx.message.channel}")
            await message.add_reaction("\U0000274C")
            if not isinstance(ctx.message.channel, nextcord.DMChannel):
                await message.delete(delay=3)
            embed = await command_error(ctx, bot)
            await ctx.message.author.send(embed=embed)


@bot.command()
async def r(ctx):
    process_msg = await discord.send_request_process_msg(ctx)
    requester = await discord.get_requester(ctx)
    if not isinstance(ctx.channel, nextcord.DMChannel):
        await ctx.message.delete(delay=3)
    guild = await bot.fetch_guild(974431346850140201)
    member = await guild.fetch_member(ctx.author.id)
    role = member.get_role(1134395525727272991)
    if role:
        await main(ctx, process_msg, requester, _configuration)
    else:
        logging.getLogger(__name__).info(f"discord.py - User {ctx.message.author} does not have the appropriate role")
        await discord.role_deny_request_update_process_msg(process_msg)


async def loop():
    times = generate_runtimes()
    logging.getLogger(__name__).info(f"main.py - Runtime schedule:\n\t{times}")
    while True:
        if datetime.time(datetime.utcnow()).strftime("%H:%M:%S") in times:
            try:
                await main(None, None, None, _configuration)
            except Exception as e:
                logging.getLogger(__name__).info(f"main.py - Traceback:\n\t{traceback.print_exc()}")

        await asyncio.sleep(1)


@bot.event
async def on_ready():
    """Prints a message to the logs when connection to Discord is established (bot is running)"""
    logging.getLogger(__name__).info(f"main.py - Discord connection established")


@bot.command()
async def s(ctx, *args):
    """This function treats a Discord message (context) as a line of arguments and attempts to create a new user subscription"""
    logging.getLogger(__name__).info(f"main.py - Subscription request received from {ctx.message.author}: {args}")
    process_msg = await discord.send_subscription_process_msg(ctx)
    valid_user_data, invalid_user_data = await User.discord(_configuration, process_msg, "subscribe", str(ctx.message.author), int(ctx.message.author.id), *args)
    if valid_user_data:
        process_msg = await discord.update_subscription_process_msg(process_msg, 3, None)
        await user.write_db(valid_user_data)
        guild = await bot.fetch_guild(974431346850140201)
        member = await guild.fetch_member(ctx.author.id)
        role = nextcord.utils.get(guild.roles, name="tester")
        await member.add_roles(role)
        await discord.update_subscription_process_msg(process_msg, 4, invalid_user_data)
        logging.getLogger(__name__).info(f"main.py - Subscription successful for {ctx.message.author}: {valid_user_data}\n\tDenied for: {invalid_user_data}")
    else:
        await discord.deny_subscription(process_msg)
        logging.getLogger(__name__).info(f"main.py - Subscription denied for {ctx.message.author}: {args}")

    if not isinstance(ctx.channel, nextcord.DMChannel):
        await ctx.message.delete(delay=3)


@bot.command()
async def u(ctx, *args):
    """This function treats a Discord message (context) as a line of arguments and attempts to unsubscribe the user"""
    logging.getLogger(__name__).info(f"main.py - Unubscription request received from {ctx.message.author}: {args}")
    pass


def run_uvicorn():
    host = "127.0.0.1"
    port = 8000
    log_level = 'debug'
    logging.getLogger(__name__).info(f"main.py - Uvicorn running on {host}:{port}")
    uvicorn.run(database_api, host=host, port=port, log_level=log_level, log_config=f'assets/data/logs/bot/uvicorn.ini')


if __name__ == "__main__":

    bot.loop.create_task(loop())

    # Create a thread for running uvicorn
    uvicorn_thread = threading.Thread(target=run_uvicorn)
    uvicorn_thread.start()
    bot.loop.run_until_complete(bot.start(discord_token, reconnect=True))
