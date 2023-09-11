import asyncio

import aiohttp
from aiohttp import client_exceptions
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
                await asyncio.sleep(0)

                if resp.status == 200:
                    data = await resp.json()
                    return data

                else:
                    return None

    async def text(self, configuration: dict):

        timeout = aiohttp.ClientTimeout(total=configuration["general"]["request timeout (sec)"])
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, timeout=timeout) as resp:
                await asyncio.sleep(0)
                if resp.status == 200:
                    text = await resp.text()
                    obj = schemas.NodeMetrics.from_txt(text)
                    del text
                    return obj
                else:
                    return None


async def safe_request(request_url: str, configuration: dict):
    retry_count = 0
    while True:
        try:
            if "metrics" in request_url.split("/"):
                data = await Request(request_url).text(configuration)
            else:
                data = await Request(request_url).json(configuration)
            if retry_count >= configuration['general']['request retry (count)']:
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


async def get_user_ids(layer, requester, _configuration):
    """RETURNS A LIST/SET OF TUPLES CONTAINING ID, IP, PORT (PER LAYER)"""
    try:
        if requester is None:
            data = await Request(f"http://127.0.0.1:8000/user/ids/layer/{layer}").json(_configuration)
        else:
            data = await Request(f"http://127.0.0.1:8000/user/ids/contact/{requester}/layer/{layer}").json(_configuration)
    except asyncio.TimeoutError:
        logging.getLogger(__name__).warning(
            f"api.py - Localhost timeout while getting user id")
        await asyncio.sleep(1)
    else:
        return data



async def locate_node(_configuration, requester, id_, ip, port):
    """Locate every subscription where ID is id_
    return await dask_client.compute(subscriber_dataframe[subscriber_dataframe.id == id_])"""
    while True:
        try:
            data = await Request(f"http://127.0.0.1:8000/user/ids/{id_}/{ip}/{port}").json(_configuration)
        except asyncio.TimeoutError:
            logging.getLogger(__name__).warning(
                f"api.py - Localhost timeout while locating node")
            await asyncio.sleep(1)
        else:
            return data


