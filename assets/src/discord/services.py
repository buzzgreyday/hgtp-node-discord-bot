import os
from os import getenv
from dotenv import load_dotenv
import nextcord
from nextcord.ext import commands

load_dotenv()

"""LOAD DISCORD SERVER TOKEN FROM ENVIRONMENT"""
discord_token = getenv("DISCORD_TOKEN")

"""GUILD CONFIG"""
dev_env = os.getenv("NODEBOT_DEV_ENV")
NODEBOT_GUILD = 974431346850140201
NODEBOT_DEV_GUILD = 1281616185170853960
slash_commands_guild_id = [int(NODEBOT_DEV_GUILD)] if dev_env else None
guild_id = int(NODEBOT_DEV_GUILD) if dev_env else int(NODEBOT_GUILD)

description = """Discord app by Buzz Greyday"""
intents = nextcord.Intents.all()
intents.reactions = True
intents.members = True

bot = commands.Bot(command_prefix="!", description=description, intents=intents, default_guild_ids=[int(NODEBOT_DEV_GUILD)])
