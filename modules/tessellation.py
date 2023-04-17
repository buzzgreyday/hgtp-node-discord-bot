import aiohttp
import logging
from datetime import datetime
from modules import request

class Request:
    def __init__(self, url):
        self.url = url

    async def json(self, configuration):
        timeout = aiohttp.ClientTimeout(total=configuration["request"]["timeout"])
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.debug(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST SUCCEEDED")
                    return data
                elif resp.status == 503:
                    logging.debug(
                        f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED: SERVICE UNAVAILABLE")
                    return 503
                else:
                    logging.critical(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED")
            del resp
            await session.close()

async def latest_version_github(configuration):
    data = await request.safe(
        f"{configuration['request']['url']['github']['api repo url']}/"
        f"{configuration['request']['url']['github']['url endings']['tessellation']['latest release']}", configuration)
    return data["tag_name"][1:]