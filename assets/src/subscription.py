import asyncio
import datetime
import logging
import re
import traceback
from datetime import datetime, timezone
from typing import List, Tuple, Any

import aiohttp
import nextcord
import yaml

from assets.src import api
from assets.src.discord.services import bot
from assets.src.encode_decode import id_to_dag_address
from assets.src import schemas

# Define regex constants
IP_REGEX = r'^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$'
EMAIL_REGEX = re.compile(r'([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+')

# Modal for subscribing IP and Ports
class SubscribeModal(nextcord.ui.Modal):
    def __init__(self):
        super().__init__(title="Subscribe IP and Ports")

        # Define TextInput fields
        self.ip = nextcord.ui.TextInput(label="IP Address", placeholder="Enter the IP address", required=True)
        self.l0_ports = nextcord.ui.TextInput(label="L0 Ports", placeholder="Enter the ports (comma-separated)", required=True)
        self.l1_ports = nextcord.ui.TextInput(label="L1 Ports", placeholder="Enter the ports (comma-separated)", required=True)
        self.email = nextcord.ui.TextInput(label="Email Address", placeholder="Enter your email address", required=True)

        # Add TextInput fields to the modal
        self.add_item(self.ip)
        self.add_item(self.l0_ports)
        self.add_item(self.l1_ports)
        self.add_item(self.email)

    async def callback(self, interaction: nextcord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            await interaction.followup.send(content="### **Nodebot is now performing checks - please wait...**", ephemeral=True)

            with open("config.yml", "r") as file:
                configuration = yaml.safe_load(file)
                await process_subscription(interaction, configuration)

        except ValueError as e:
            print("Validation Error:", e)
        except Exception as e:
            print(f"An error occurred: {traceback.format_exc()}")
            await interaction.followup.send(
                content=f"### **An unknown error occurred while processing your request - please contact an admin!**\n```Error: {e}```",
                ephemeral=True
            )


async def process_subscription(interaction, configuration):
    try:
        await discord_subscription(
            interaction=interaction,
            configuration=configuration,
            name=interaction.user.name,
            discord=interaction.user.id,
            email=interaction.modal.email.value,
            ip=interaction.modal.ip.value,
            l0_ports=interaction.modal.l0_ports.value.split(","),
            l1_ports=interaction.modal.l1_ports.value.split(","),
        )
    except ValueError as e:
        await interaction.followup.send(f"Validation Error: {str(e)}", ephemeral=True)


async def attempt_discord_dm(message: str, discord_id: int) -> bool:
    try:
        user = await bot.fetch_user(discord_id)
        await user.send(message)
        return True
    except nextcord.Forbidden:
        return False


async def get_id(session, ip: str, port: str, mode: str, configuration: dict) -> str:
    """Get node ID by IP and port."""
    if mode == "subscribe":
        try:
            node_data, _ = await api.safe_request(session, f"http://{ip}:{port}/node/info", configuration)
            return node_data["id"] if node_data else None
        except Exception as e:
            print(f"Error fetching node ID: {e}")
            return None
    return None


async def discord_subscription(interaction, configuration, name: str, discord: int, email: str, ip: str, l0_ports: List[str], l1_ports: List[str]):
    """Process the Discord subscription."""
    async def validate_ports(l0_ports: List[str], l1_ports: List[str]) -> tuple[Any]:
        """Validate and process ports."""
        async def _process_ports(port: str, layer: int) -> dict:
            if port.isdigit():
                async with aiohttp.ClientSession() as session:
                    id_ = await get_id(session=session, ip=ip, port=port, mode="subscribe", configuration=configuration)
                if id_ is None:
                    return schemas.User
                wallet = id_to_dag_address(id_)
                return generate_user_data(id_, port, layer, "online", wallet)
            else:
                return generate_user_data(None, port, layer, "invalid port")

        tasks = []
        for port in l0_ports:
            tasks.append(_process_ports(port, 0))
        for port in l1_ports:
            tasks.append(_process_ports(port, 1))
        return await asyncio.gather(*tasks)

    if not re.fullmatch(EMAIL_REGEX, email):
        await interaction.followup.send(f"### **Check failed! Invalid email: {email}**", ephemeral=True)
        raise ValueError("Invalid email")

    if not re.match(IP_REGEX, ip):
        await interaction.followup.send(f"### **Check failed! Invalid IP: {ip}**", ephemeral=True)
        raise ValueError("Invalid IP")

    # Validate ports
    data = await validate_ports(l0_ports=l0_ports, l1_ports=l1_ports)

    # Fetch existing subscription
    async with aiohttp.ClientSession() as session:
        subscription_data, _ = await api.Request(session, f"http://127.0.0.1:8000/get/user/{ip}").db_json()

    if subscription_data:
        await handle_existing_subscription(subscription_data, data)
    else:
        print("No subscriptions present, proceeding with subscription.")
        # Subscribe all ports


async def handle_existing_subscription(existing_data: dict, new_data: List[dict]):
    """Handle cases where IP already has subscriptions."""
    print("IP is already subscribed, filtering valid ports")
    # Implement logic to filter existing ports and merge subscription data



def generate_user_data(
        email: str,
        discord_id: int,
        discord_handle: str,
        ip: str,
        port: str,
        layer: int,
        status: str = None,
        id: str = None,
        customer_id: str = None,
        subscription_id: str = None,
        wallet: str = None,
        index: int = None,
        discord_dm_allowed: bool = True,
        datetime: datetime = datetime.now(timezone.utc)
) -> dict:
    """Generate user data dictionary."""
    return {
        "datetime": datetime.now(timezone.utc),
        "index": index,
        "ip": ip,
        "id": id,
        "wallet": wallet,
        "public_port": port,
        "layer": layer,
        "email": email,
        "customer_id": customer_id,
        "subscription_id": subscription_id,
        "discord_dm_allowed": discord_dm_allowed,
        "discord_id": discord_id,
        "discord_handle": discord_handle,
        "custom": status
    }
