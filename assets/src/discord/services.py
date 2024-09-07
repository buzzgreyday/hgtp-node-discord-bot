import os
from os import getenv
from dotenv import load_dotenv
import nextcord
from nextcord.ext import commands

load_dotenv()
dev_env = os.getenv("NODEBOT_DEV_ENV")

"""LOAD DISCORD SERVER TOKEN FROM ENVIRONMENT"""
discord_token = getenv("DISCORD_TOKEN") if not dev_env else getenv("DISCORD_DEV_TOKEN")

"""GUILD CONFIG"""
NODEBOT_GUILD = 974431346850140201
NODEBOT_DEV_GUILD = 1281616185170853960
guild_id = [int(NODEBOT_DEV_GUILD)] if dev_env else None

description = """Discord app by Buzz Greyday"""
intents = nextcord.Intents.all()
intents.reactions = True
intents.members = True

bot = commands.Bot(command_prefix="!", description=description, intents=intents)
