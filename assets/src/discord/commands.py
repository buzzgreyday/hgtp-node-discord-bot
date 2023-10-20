import asyncio
import logging

import yaml

import assets.src.database.database
from assets.src import user, run_process
from assets.src.database import models
from assets.src.discord import discord
from assets.src.discord.services import bot
from assets.src.schemas import User

import nextcord
from nextcord import SelectOption
from nextcord.ui import Select


class SelectMenu(Select):
    def __init__(self, msg, values):
        # Define the options for the SelectMenu
        options = [SelectOption(label=val, value=val) for val in values]

        # Initialize the SelectMenu with the options and a placeholder
        super().__init__(placeholder=msg, options=options)
        self.selected_value = None

    async def callback(self, interaction):
        # This method is called when the user selects an option
        # You can access the selected option with self.values[0]
        self.selected_value = self.values[0]



def setup(bot):
    pass


@bot.slash_command(
    name="unsubscribe",
    description="Unsubscribe from an IP address.",
    guild_ids=[974431346850140201],
    dm_permission=True,)
async def unsubscibe_menu(interaction):
    """This is a slash_command that sends a View() that contains a SelectMenu and a button to confirm user selection"""

    async def on_button_click(interaction):
        # Check if the port matches the subscribed IP
        entries = []
        for data in lst:

            if (str(interaction.user) == data["name"]) and (ip_menu.selected_value == "All") and (port_menu.selected_value in ("All", None)):
                entry = models.UserModel(**data)
                entries.append(entry)
                logging.getLogger(__name__).info(
                    f"main.py - Unubscription request accepted from {str(interaction.user)}: {ip_menu.selected_value}:{port_menu.selected_value}"
                )
                print(f"Unsubscribe: {data['name'], data['ip'], data['public_port']}")

            elif (str(interaction.user) == data["name"]) and (ip_menu.selected_value == data["ip"]) and (port_menu.selected_value == str(data["public_port"])):
                entry = models.UserModel(**data)
                entries.append(entry)
                logging.getLogger(__name__).info(
                    f"main.py - Unubscription request accepted from {str(interaction.user)}: {ip_menu.selected_value}:{port_menu.selected_value}"
                )
                print(f"Unsubscribe: {data['name'], data['ip'], data['public_port']}")
                await interaction.response.send_message(
                    content=f"You chose {ip_menu.selected_value, port_menu.selected_value}", ephemeral=True)

            elif (str(interaction.user) == data["name"]) and (ip_menu.selected_value == data["ip"]) and (port_menu.selected_value in ("All", None)):
                entry = models.UserModel(**data)
                entries.append(entry)
                logging.getLogger(__name__).info(
                    f"main.py - Unubscription request accepted from {str(interaction.user)}: {ip_menu.selected_value}:{port_menu.selected_value}"
                )
                print(f"Unsubscribe: {data['name'], data['ip'], data['public_port']}")

            logging.getLogger(__name__).info(
                f"main.py - Unubscription request denied from {str(interaction.user)}: {ip_menu.selected_value}:{port_menu.selected_value}"
            )
        if entries:
            await user.delete_db(entries)

    lst, resp_status = await assets.src.api.Request(f"http://127.0.0.1:8000/user/{str(interaction.user)}").db_json()
    if lst:
        ips = ["All"]
        ports = ["All"]
        for data in lst:
            ips.append(data["ip"])
            ports.append(data["public_port"])
        # This is the slash command that sends the message with the SelectMenu
        # Create a view that contains the SelectMenu
        view = nextcord.ui.View()
        ip_menu = SelectMenu("Select the IP you want to unsubscribe", set(ips))
        port_menu = SelectMenu("Select port", set(ports))
        button = nextcord.ui.Button(style=nextcord.ButtonStyle.primary, label="Confirm")
        button.callback = on_button_click  # Set the callback for the button
        view.add_item(ip_menu)
        view.add_item(port_menu)
        view.add_item(button)
        # Send the message with the view
        await interaction.response.send_message(content="Unsubscribe", ephemeral=True, view=view)
    else:
        await interaction.response.send_message(
            content=f"No subscription found", ephemeral=True)


@bot.slash_command(
    name="verify",
    description="Verify your server settings to gain access",
    guild_ids=[974431346850140201],
    dm_permission=True,
)
async def verify(interaction=nextcord.Interaction):
    try:
        await interaction.user.send(f"Checking Discord server settings...")
    except nextcord.Forbidden:
        await interaction.send(
            content=f"{interaction.user.mention}, to gain access you need to navigate to `Privacy Settings` an enable `Direct Messages` from server members. If you experience issues, please contact an admin.",
            ephemeral=True,
        )
        logging.getLogger(__name__).info(
            f"discord.py - Verification of {interaction.user} denied"
        )
    else:
        guild = await bot.fetch_guild(974431346850140201)
        role = nextcord.utils.get(guild.roles, name="verified")
        if role:
            await interaction.user.add_roles(role)
            await interaction.send(
                content=f"{interaction.user.mention}, your settings were verified!",
                ephemeral=True,
            )
            await interaction.user.send(
                content=f"{interaction.user.mention}, your settings were verified!"
            )
    return


@bot.command()
async def r(ctx):
    with open("config.yml", "r") as file:
        _configuration = yaml.safe_load(file)
        process_msg = await discord.send_request_process_msg(ctx)
        if process_msg:
            requester = await discord.get_requester(ctx)
            if not isinstance(ctx.channel, nextcord.DMChannel):
                await ctx.message.delete(delay=3)
            guild, member, role = await discord.return_guild_member_role(bot, ctx)
            if role:
                fut = []
                for layer in (0, 1):
                    fut.append(
                        asyncio.create_task(
                            run_process.request_check(
                                process_msg, layer, requester, _configuration
                            )
                        )
                    )
                for task in fut:
                    await task
            else:
                logging.getLogger(__name__).info(
                    f"discord.py - User {ctx.message.author} does not have the appropriate role"
                )
                await discord.messages.subscriber_role_deny_request(process_msg)
        else:
            if not isinstance(ctx.channel, nextcord.DMChannel):
                await ctx.message.delete(delay=3)
            logging.getLogger(__name__).info(
                f"discord.py - User {ctx.message.author} does not allow DMs"
            )


@bot.command()
async def s(ctx, *args):
    """This function treats a Discord message (context) as a line of arguments and attempts to create a new user subscription"""
    with open("config.yml", "r") as file:
        _configuration = yaml.safe_load(file)
        logging.getLogger(__name__).info(
            f"main.py - Subscription request received from {ctx.message.author}: {args}"
        )
        process_msg = await discord.send_subscription_process_msg(ctx)
        valid_user_data, invalid_user_data = await User.discord(
            _configuration,
            process_msg,
            "subscribe",
            str(ctx.message.author),
            int(ctx.message.author.id),
            *args,
        )
        if valid_user_data:
            process_msg = await discord.update_subscription_process_msg(
                process_msg, 3, None
            )
            await user.write_db(valid_user_data)
            guild, member, role = await discord.return_guild_member_role(bot, ctx)
            await member.add_roles(role)
            await discord.update_subscription_process_msg(
                process_msg, 4, invalid_user_data
            )
            logging.getLogger(__name__).info(
                f"main.py - Subscription successful for {ctx.message.author}: {valid_user_data}\n\tDenied for: {invalid_user_data}"
            )
        else:
            await discord.deny_subscription(process_msg)
            logging.getLogger(__name__).info(
                f"main.py - Subscription denied for {ctx.message.author}: {args}"
            )

        if not isinstance(ctx.channel, nextcord.DMChannel):
            await ctx.message.delete(delay=3)
