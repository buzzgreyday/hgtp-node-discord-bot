import asyncio
import logging

from assets.src import exception
from assets.src.discord import discord
from assets.src.discord.services import bot

import nextcord

def setup(bot):
    pass

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