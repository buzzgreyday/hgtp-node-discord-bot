from os import getenv

import nextcord
from nextcord.ext import commands

"""LOAD DISCORD SERVER TOKEN FROM ENVIRONMENT"""
discord_token = getenv("HGTP_SPIDR_DISCORD_TOKEN")

description = '''Bot by hgtp_Michael'''
intents = nextcord.Intents.all()
intents.members = True

bot = commands.Bot(command_prefix='!', description=description, intents=intents)
