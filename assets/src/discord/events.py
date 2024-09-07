import logging

from assets.src.discord import discord, messages
from assets.src.discord.services import bot

import nextcord

from services import dev_env

NON_COMMAND_CHANNELS = (977357753947402281, 974431346850140204, 1030007676257710080, 1134396471639277648)
VERIFY_CHANNEL = 1136386732628115636

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
        if ctx.message.channel.id in NON_COMMAND_CHANNELS:
            # IGNORE INTERPRETING MESSAGES IN THESE CHANNELS AS COMMANDS
            logging.getLogger("commands").info(
                f"Module: assets/src/discord/events.py\n"
                f"Message: \"{ctx.message.content}\"\n"
                f"Channel: Non-command"
            )
        elif ctx.message.channel.id == VERIFY_CHANNEL:
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
                embed = await messages.command_error(ctx, bot)

                await ctx.message.author.send(embed=embed)
            else:
                pass


@bot.event
async def on_ready():
    """Prints a message to the logs when a connection to Discord is established (bot is running)"""
    logging.getLogger("app").info(f"events.py - Discord connection established")
