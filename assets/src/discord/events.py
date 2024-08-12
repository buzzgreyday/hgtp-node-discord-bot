import logging
import os

from assets.src import exception
from assets.src.discord import discord
from assets.src.discord.services import bot

import nextcord

dev_env = os.getenv("NODEBOT_DEV_ENV")

def setup(bot):
    pass


@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    ctx = await bot.get_context(message)
    if ctx.valid:
        logging.getLogger("commands").info(
            f"Module: assets/src/discord/events.py\n"
            f"Message: \"{ctx.message.content}\" - {ctx.message.author}\n"
            f"Channel: {ctx.message.channel}"
        )
        await bot.process_commands(message)
    else:
        if ctx.message.channel.id in (
            977357753947402281,
            974431346850140204,
            1030007676257710080,
            1134396471639277648,
        ):
            # IGNORE INTERPRETING MESSAGES IN THESE CHANNELS AS COMMANDS
            logging.getLogger("commands").info(
                f"Module: assets/src/discord/events.py\n"
                f"Message: \"{ctx.message.content}\"\n"
                f"Channel: Non-command"
            )
        elif ctx.message.channel.id == 1136386732628115636:
            if not dev_env:
                logging.getLogger("commands").info(
                    f"Module: assets/src/discord/events.py\n"
                    f"Message: \"{ctx.message.content}\" - {ctx.message.author}\n"
                    f"Channel: Verify"
                )
                await discord.delete_message(ctx)
            else:
                pass
        else:
            if not dev_env:
                logging.getLogger("commands").info(
                    f"Module: assets/src/discord/events.py\n"
                    f"Message: \"{ctx.message.content}\" - {ctx.message.author}\n"
                    f"Channel: {ctx.message.channel}"
                )
                await message.add_reaction("\U0000274C")
                if not isinstance(ctx.message.channel, nextcord.DMChannel):
                    await message.delete(delay=3)
                embed = await exception.command_error(ctx, bot)

                await ctx.message.author.send(embed=embed)
            else:
                pass


@bot.event
async def on_ready():
    """Prints a message to the logs when a connection to Discord is established (bot is running)"""
    logging.getLogger("app").info(f"events.py - Discord connection established")
