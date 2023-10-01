import asyncio
import traceback

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
                timeout=aiohttp.ClientTimeout(
                    total=configuration["general"]["request timeout (sec)"]
                ),
            ) as resp:
                await asyncio.sleep(0)
                if resp.status == 200:
                    data = await resp.json()
                    return data, resp.status

                else:
                    return None, resp.status

    async def db_json(self, configuration: dict):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as resp:
                await asyncio.sleep(0)
                if resp.status == 200:
                    data = await resp.json()
                    return data, resp.status

                else:
                    return None, resp.status

    async def text(self, configuration: dict):
        timeout = aiohttp.ClientTimeout(
            total=configuration["general"]["request timeout (sec)"]
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, timeout=timeout) as resp:
                await asyncio.sleep(0)
                if resp.status == 200:
                    text = await resp.text()
                    obj = schemas.NodeMetrics.from_txt(text)
                    del text
                    return obj, resp.status
                else:
                    return None, resp.status


async def safe_request(request_url: str, configuration: dict):
    retry_count = 0
    status_code = None
    while True:
        try:
            if "metrics" in request_url.split("/"):
                data, status_code = await Request(request_url).text(configuration)
            else:
                data, status_code = await Request(request_url).json(configuration)
            if retry_count >= configuration["general"]["request retry (count)"]:
                return None, status_code
            elif data is not None:
                return data, status_code
            else:
                retry_count += 1
                await asyncio.sleep(
                    configuration["general"]["request retry interval (sec)"]
                )
        except (
            asyncio.exceptions.TimeoutError,
            aiohttp.client_exceptions.ClientConnectorError,
            aiohttp.client_exceptions.ClientOSError,
            aiohttp.client_exceptions.ServerDisconnectedError,
            aiohttp.client_exceptions.ClientPayloadError,
        ):
            if retry_count >= configuration["general"]["request retry (count)"]:
                return None, status_code
            retry_count += 1
            await asyncio.sleep(
                configuration["general"]["request retry interval (sec)"]
            )
            logging.getLogger(__name__).warning(
                f"api.py - {request_url} returned \"{status_code}\" ({retry_count}/{configuration['general']['request retry (count)']})"
            )
        except (
            aiohttp.client_exceptions.InvalidURL,
        ) as e:
            logging.getLogger(__name__).warning(
                f"api.py - {request_url} returned \"{status_code}\" ({retry_count}/{configuration['general']['request retry (count)']})"
            )
            return None, status_code


async def get_user_ids(layer, requester, _configuration):
    """RETURNS A LIST/SET OF TUPLES CONTAINING ID, IP, PORT (PER LAYER)"""
    while True:
        try:
            if requester is None:
                data, resp_status = await Request(
                    f"http://127.0.0.1:8000/user/ids/layer/{layer}"
                ).db_json(_configuration)
            else:
                data, resp_status = await Request(
                    f"http://127.0.0.1:8000/user/ids/contact/{requester}/layer/{layer}"
                ).db_json(_configuration)
        except asyncio.TimeoutError:
            logging.getLogger(__name__).error(
                f"api.py - get_user_ids timeout timeout error!\n\t{traceback.format_exc()}"
            )
            await asyncio.sleep(6)
        else:
            if resp_status == 200:
                return data
            else:
                logging.getLogger(__name__).warning(
                    f"api.py - localhost error: http://127.0.0.1:8000/user/ids/contact/{requester}/layer/{layer} return status {resp_status}"
                )
                await asyncio.sleep(3)


async def locate_node(_configuration, requester, id_, ip, port):
    """Locate every subscription where ID is id_
    return await dask_client.compute(subscriber_dataframe[subscriber_dataframe.id == id_])
    """
    while True:
        try:
            data, resp_status = await Request(
                f"http://127.0.0.1:8000/user/ids/{id_}/{ip}/{port}"
            ).db_json(_configuration)
        except asyncio.TimeoutError:
            logging.getLogger(__name__).warning(
                f"api.py - localhost error: http://127.0.0.1:8000/user/ids/{id_}/{ip}/{port} locate node timeout"
            )
            await asyncio.sleep(6)
        else:
            if resp_status == 200:
                return data
            else:
                logging.getLogger(__name__).warning(
                    f"api.py - localhost error: http://127.0.0.1:8000/user/ids/{id_}/{ip}/{port} returned status {resp_status}"
                )
                await asyncio.sleep(6)
