import nextcord
import logging
from datetime import datetime


async def set_active_presence(bot):
    try:
        logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CHANGING PRESENCE TO \"ACTIVE\"")
        return await bot.change_presence(activity=nextcord.Activity(type=nextcord.ActivityType.watching, name=f'nodes since {datetime.utcnow().strftime("%H:%M")} UTC'), status=nextcord.Status.online)
    except Exception:
        logging.warning(f"{datetime.utcnow().strftime('%H:%M:%S')} - ATTEMPTING TO RECONNECT BEFORE CHANGING PRESENCE TO \"ACTIVE\"")
        await bot.wait_until_ready()
        await bot.connect(reconnect=True)
        return await bot.change_presence(activity=nextcord.Activity(type=nextcord.ActivityType.watching, name=f'nodes since {datetime.utcnow().strftime("%H:%M")} UTC'), status=nextcord.Status.online)
