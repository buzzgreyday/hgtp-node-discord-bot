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

async def validator_data(configuration: dict):
    async def make_request(validator_urls: list) -> list[dict]:
        for url in validator_urls:
            validator_network_data = await request.safe(str(url), configuration)
            if validator_network_data is None:
                continue
            elif validator_network_data is not None:
                return validator_network_data["data"]


    validator_testnet_data = await make_request(configuration['request']['url']['validator info']['testnet']['url'])
    validator_mainnet_data = await make_request(configuration['request']['url']['validator info']['mainnet']['url'])

    return validator_mainnet_data, validator_testnet_data