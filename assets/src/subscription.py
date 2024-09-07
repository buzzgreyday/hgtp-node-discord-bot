import datetime
import logging
import re
import traceback
from datetime import datetime
from typing import List

import aiohttp
import nextcord
import yaml

from assets.src import api
from assets.src.encode_decode import id_to_dag_address

IP_REGEX = r'^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$'
EMAIL_REGEX = re.compile(r'([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+')


class SubscribeModal(nextcord.ui.Modal):
    def __init__(self):
        super().__init__(title="Subscribe IP and Ports")

        # TextInput fields
        self.ip = nextcord.ui.TextInput(
            label="IP Address",
            placeholder="Enter the IP address",
            required=True,
        )
        self.l0_ports = nextcord.ui.TextInput(
            label="L0 Ports",
            placeholder="Enter the ports (comma-separated)",
            required=True,
        )
        self.l1_ports = nextcord.ui.TextInput(
            label="L1 Ports",
            placeholder="Enter the ports (comma-separated)",
            required=True,
        )
        self.email = nextcord.ui.TextInput(
            label="Email Address",
            placeholder="Enter your email address",
            required=True,
        )

        # Add TextInput fields to the modal
        self.add_item(self.ip)
        self.add_item(self.l0_ports)
        self.add_item(self.l1_ports)
        self.add_item(self.email)

    async def callback(self, interaction: nextcord.Interaction):
        try:
            # Defer the interaction to give you more time to process
            await interaction.response.defer(ephemeral=True)
            await interaction.followup.send(
                content="### **Nodebot is now performing checks - please wait...**",
                ephemeral=True
            )
            with (open("config.yml", "r") as file):
                _configuration = yaml.safe_load(file)
                try:
                    await discord_subscription(
                            interaction=interaction,
                            configuration=_configuration,
                            name=interaction.user.name,
                            discord=interaction.user.id,
                            email=self.email.value,
                            ip=self.ip.value,
                            l0_ports=[port for port in str(self.l0_ports.value).split(",")],
                            l1_ports=[port for port in str(self.l1_ports.value).split(",")]
                    )

                except ValueError as e:
                    print("IP or email isn't valid")
                else:
                    pass
        except Exception as e:
            # Handle potential errors
            print(f"An error occurred: {e}")
            # After processing is done, send a follow-up message to the user
            await interaction.followup.send(
                content="### **An unknown error occurred while processing your request - please contact an admin!**",
                ephemeral=True  # Set to True if you want it to be visible only to the user
            )


async def attempt_discord_dm(message: str, discord_id: int):
    pass

async def get_id(session, ip: str, port: str, mode, configuration):
    """Will need refactoring before metagraph release. Some other way to validate node?"""
    if mode == "subscribe":
        print(f"Requesting: {ip}:{port}")
        try:
            node_data, status_code = await api.safe_request(
                session, f"http://{ip}:{port}/node/info", configuration
            )
        except Exception as e:
            print(e)
            return
        else:
            return str(node_data["id"]) if node_data is not None else None
    else:
        return None

async def discord_subscription(
        interaction,
        configuration,
        name: str,
        discord: int,
        email: str,
        ip: str,
        l0_ports: List[str],
        l1_ports: List[str]
):
    async def validate_ports(l0_ports, l1_ports):
        async def _process_ports(processed_port, layer) -> List[dict]:
            user_data = []
            if port.isdigit():
                async with aiohttp.ClientSession() as session:
                    id_ = await get_id(
                        session=session, ip=ip, port=processed_port, mode="subscribe", configuration=configuration
                    )
                if id_ is None:
                    print("ID was not retrievable, make sure your node is online!")
                    user_data.extend(
                        {
                            "datetime": datetime.now(datetime.timezone.utc),
                            "index": None,
                            "ip": ip,
                            "id": None,
                            "wallet": None,
                            "public_port": processed_port,
                            "layer": layer,
                            "email": email,
                            "customer_id": None,
                            "discord_dm_allowed": None,
                            "discord_id": discord,
                            "discord_handle": name,
                            "custom": "offline"
                        }
                    )
                else:
                    wallet = id_to_dag_address(id_)
                    user_data.extend(
                        {
                            "datetime": datetime.now(datetime.timezone.utc),
                            "index": None,
                            "ip": ip,
                            "id": id_,
                            "wallet": wallet,
                            "public_port": processed_port,
                            "layer": layer,
                            "email": email,
                            "customer_id": None,
                            "discord_dm_allowed": None,
                            "discord_id": discord,
                            "discord_handle": name,
                            "custom": "online"
                        }
                    )
            else:
                print("Not a valid port!")
                user_data.extend(
                    {
                        "datetime": datetime.now(datetime.timezone.utc),
                        "index": None,
                        "ip": ip,
                        "id": None,
                        "wallet": None,
                        "public_port": processed_port,
                        "layer": layer,
                        "email": email,
                        "customer_id": None,
                        "discord_dm_allowed": None,
                        "discord_id": discord,
                        "discord_handle": name,
                        "custom": "invalid port"
                    }
                )
            return user_data
        data = []
        for port in l0_ports:
            data.extend(await _process_ports(port, 0))
        for port in l1_ports:
            data.extend(await _process_ports(port, 1))
        return data

    if not re.fullmatch(EMAIL_REGEX, email):
        await interaction.followup.send(
            content=f"### **Check failed! Invalid email: {email}**",
            ephemeral=True  # Set to True if you want it to be visible only to the user
        )
        raise ValueError("Not a valid email")
    if not re.match(IP_REGEX, ip):
        await interaction.followup.send(
            content=f"### **Check failed! Invalid IP: {ip}**",
            ephemeral=True  # Set to True if you want it to be visible only to the user
        )
        raise ValueError("Not a valid IP")

    data = await validate_ports(l0_ports=l0_ports, l1_ports=l1_ports)

    # Check if IP is subscribed
    async with aiohttp.ClientSession() as session:
        subscription_data, resp_status = await api.Request(
            session, f"http://127.0.0.1:8000/get/user/{ip}"
        ).db_json()

    print("Subscribed Data:", subscription_data)
    print("New Data:", data)
    if subscription_data:
        print("IP is already subscribed: discard existing subscribed valid ports")
        # Check if ports differ; subtract pop the subscribed data from the new ports and check these
        # Extend new valid_user_data with the already subscribed data ("current_period_end" and "customer_id")
    else:
        print("No subscriptions present: subscribe all")
        # For dev purposes

        # Check all ports



    # await user.write_db(valid_user_data)

    # After processing is done, send a follow-up message to the user
    await interaction.followup.send(
        content="### **Checks passed!**",
        ephemeral=True  # Set to True if you want it to be visible only to the user
    )
