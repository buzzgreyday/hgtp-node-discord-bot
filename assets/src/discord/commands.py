import asyncio
import logging

import yaml

from assets.src import user, run_process
from assets.src.discord import discord
from assets.src.discord.services import bot
from assets.src.schemas import User

import nextcord

def setup(bot):
    pass


@bot.slash_command(name="verify",
                   description="Verify your server settings to gain access",
                   guild_ids=[974431346850140201],
                   dm_permission=True)
async def verify(interaction=nextcord.Interaction):
    try:
        await interaction.user.send(f"Checking Discord server settings...")
    except nextcord.Forbidden:
        await interaction.send(
            content=f"{interaction.user.mention}, to gain access you need to navigate to `Privacy Settings` an enable `Direct Messages` from server members. If you experience issues, please contact an admin.",
            ephemeral=True)
        logging.getLogger(__name__).info(f"discord.py - Verification of {interaction.user} denied")
    else:
        guild = await bot.fetch_guild(974431346850140201)
        role = nextcord.utils.get(guild.roles, name="verified")
        if role:
            await interaction.user.add_roles(role)
            await interaction.send(content=f"{interaction.user.mention}, your settings were verified!",
                                   ephemeral=True)
            await interaction.user.send(content=f"{interaction.user.mention}, your settings were verified!")
    return

@bot.command()
async def r(ctx):
    with open('config.yml', 'r') as file:
        _configuration = yaml.safe_load(file)
        process_msg = await discord.send_request_process_msg(ctx)
        if process_msg:
            requester = await discord.get_requester(ctx)
            if not isinstance(ctx.channel, nextcord.DMChannel):
                await ctx.message.delete(delay=3)
            guild, member, role = await discord.return_guild_member_role(bot, ctx)
            if role:
                fut = []
                for cluster_name in _configuration["modules"].keys():
                    for layer in _configuration["modules"][cluster_name].keys():
                        fut.append(asyncio.create_task(
                            run_process.main(ctx, process_msg, requester, cluster_name, layer, _configuration)))
                for task in fut:
                    await task
            else:
                logging.getLogger(__name__).info(
                    f"discord.py - User {ctx.message.author} does not have the appropriate role")
                await discord.messages.subscriber_role_deny_request(process_msg)
        else:
            if not isinstance(ctx.channel, nextcord.DMChannel):
                await ctx.message.delete(delay=3)
            logging.getLogger(__name__).info(f"discord.py - User {ctx.message.author} does not allow DMs")

@bot.command()
async def s(ctx, *args):
    """This function treats a Discord message (context) as a line of arguments and attempts to create a new user subscription"""
    with open('config.yml', 'r') as file:
        _configuration = yaml.safe_load(file)
        logging.getLogger(__name__).info(
            f"main.py - Subscription request received from {ctx.message.author}: {args}")
        process_msg = await discord.send_subscription_process_msg(ctx)
        valid_user_data, invalid_user_data = await User.discord(_configuration, process_msg, "subscribe",
                                                                str(ctx.message.author), int(ctx.message.author.id),
                                                                *args)
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
