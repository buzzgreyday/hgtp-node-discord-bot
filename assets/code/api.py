import asyncio
import aiohttp
from aiohttp import client_exceptions
from datetime import datetime
import logging

from assets.code import schemas


class Request:

    def __init__(self, url):
        self.url = url

    async def json(self, configuration):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    self.url,
                    timeout=aiohttp.ClientTimeout(total=configuration["general"]["request timeout (sec)"])) as resp:

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

    async def text(self, configuration: dict):

        timeout = aiohttp.ClientTimeout(total=configuration["general"]["request timeout (sec)"])
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, timeout=timeout) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    logging.debug(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST SUCCEEDED")

                    obj = schemas.NodeMetrics.from_txt(text)
                    del text
                    return obj
                elif resp.status == 503:
                    logging.debug(
                        f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED: SERVICE UNAVAILABLE")
                    return 503
                else:
                    logging.critical(f"{datetime.utcnow().strftime('%H:%M:%S')} - JSON REQUEST FAILED")
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
            logging.info(f"{datetime.utcnow().strftime('%H:%M:%S')} - CLUSTER @ {request_url} UNREACHABLE - TRIED {retry_count}/{configuration['general']['request retry (count)']}")
        except (aiohttp.client_exceptions.ClientConnectorError, aiohttp.client_exceptions.InvalidURL):
            return None
