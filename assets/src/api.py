import asyncio
import aiohttp
from aiohttp import client_exceptions
from datetime import datetime
import logging

from assets.src import schemas


class Request:

    def __init__(self, url):
        self.url = url

    async def json(self, configuration: dict):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    self.url,
                    timeout=aiohttp.ClientTimeout(total=configuration["general"]["request timeout (sec)"])) as resp:

                if resp.status == 200:
                    data = await resp.json()
                    return data

                elif resp.status == 503:
                    return 503

            del resp
            await session.close()

    async def text(self, configuration: dict):

        timeout = aiohttp.ClientTimeout(total=configuration["general"]["request timeout (sec)"])
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, timeout=timeout) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    obj = schemas.NodeMetrics.from_txt(text)
                    del text
                    return obj
                elif resp.status == 503:
                    return 503
            del resp
            await session.close()


async def safe_request(request_url: str, configuration: dict):
    retry_count = 0
    while True:
        try:
            if "metrics" in request_url.split("/"):
                data = await Request(request_url).text(configuration)
            else:
                data = await Request(request_url).json(configuration)
            if retry_count >= configuration['general']['request retry (count)'] or data == 503:
                return None
            elif data is not None:
                return data
            else:
                retry_count += 1
                await asyncio.sleep(configuration['general']['request retry interval (sec)'])
        except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientOSError,
                aiohttp.client_exceptions.ServerDisconnectedError) as e:
            if retry_count >= configuration['general']['request retry (count)']:
                return None
            retry_count += 1
            await asyncio.sleep(configuration['general']['request retry interval (sec)'])
            logging.getLogger(__name__).warning(f"api.py - {request_url} is unreachable ({retry_count}/{configuration['general']['request retry (count)']})")
        except (aiohttp.client_exceptions.ClientConnectorError, aiohttp.client_exceptions.InvalidURL):
            return None
