from os import getenv
from dotenv import load_dotenv
import nextcord
from nextcord.ext import commands

load_dotenv()

"""LOAD DISCORD SERVER TOKEN FROM ENVIRONMENT"""
discord_token = getenv("DISCORD_TOKEN")

description = '''Bot by hgtp_Michael'''
intents = nextcord.Intents.all()
intents.reactions = True
intents.members = True

bot = commands.Bot(command_prefix='!', description=description, intents=intents)

