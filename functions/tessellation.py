import asyncio
import aiohttp
from aiohttp import client_exceptions
import logging
from datetime import datetime

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
    data = None
    run_again = True
    retry_count = 0
    while run_again:
        try:
            data = await Request(
                f"{configuration['request']['url']['github']['api repo url']}/{configuration['request']['url']['github']['url endings']['tessellation']['latest release']}").json(
                configuration)
            if retry_count >= configuration['request']['max retry count']:
                run_again = False
                break
            elif data is not None:
                run_again = False
                break
            else:
                retry_count += 1
                await asyncio.sleep(configuration['request']['retry sleep'])
            # cluster_data is a list of dictionaries
        except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientOSError,
                aiohttp.client_exceptions.ServerDisconnectedError) as e:
            if retry_count >= configuration['request']['max retry count']:
                break
            retry_count += 1
            await asyncio.sleep(configuration['request']['retry sleep'])
            logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - GITHUB URL UNREACHABLE")
    return data["tag_name"][1:]

async def validator_data(configuration: dict):
    async def safe_request(validator_urls: list) -> list[dict]:
        validator_network_data = None
        run_again = True
        retry_count = 0
        while run_again:
            try:
                for url in validator_urls:
                    while retry_count < configuration['request']['max retry count']:
                        validator_network_data = await Request(str(url)).json(configuration)
                        if validator_network_data == 503:
                            break
                        elif validator_network_data is not None:
                            run_again = False
                            break
                        else:
                            await asyncio.sleep(configuration['request']['retry sleep'])
                            retry_count += 1
            except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientConnectorError,
                    aiohttp.client_exceptions.ClientOSError,
                    aiohttp.client_exceptions.ServerDisconnectedError) as e:
                if retry_count >= configuration['request']['max retry count']:
                    break
                await asyncio.sleep(configuration['request']['retry sleep'])
        return validator_network_data["data"]

    validator_testnet_data = await safe_request(configuration['request']['url']['validator info']['testnet']['url'])
    validator_mainnet_data = await safe_request(configuration['request']['url']['validator info']['mainnet']['url'])

    return validator_mainnet_data, validator_testnet_data