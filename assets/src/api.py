import asyncio
import traceback

import aiohttp
import asyncpg.exceptions
import sqlalchemy.exc
from aiohttp import client_exceptions
import logging

from assets.src import schemas


class Request:
    def __init__(self, session, url):
        self.url = url
        self.session = session

    async def _get_response(self, timeout=None):
        try:
            async with self.session.get(self.url, timeout=timeout) as resp:
                return await resp.json(), resp.status
        except aiohttp.ClientError:
            return None, None

    async def json(self, configuration: dict):
        timeout = aiohttp.ClientTimeout(total=configuration["general"]["request timeout (sec)"])
        data, status = await self._get_response(timeout)
        return data, status if status == 200 else None

    async def db_json(self, configuration=None):
        data, status = await self._get_response()
        return data, status if status == 200 else None

    async def text(self, configuration: dict):
        timeout = aiohttp.ClientTimeout(total=configuration["general"]["request timeout (sec)"])
        try:
            async with self.session.get(self.url, timeout=timeout) as resp:
                text = await resp.text()
                obj = schemas.NodeMetrics.from_txt(text)
                return obj, resp.status if resp.status == 200 else None
        except aiohttp.ClientError:
            return None, None


async def safe_request(session, request_url: str, configuration: dict):
    retry_count = 0
    status_code = None
    while retry_count < configuration["general"]["request retry (count)"]:
        try:
            request = Request(session, request_url)
            if "metrics" in request_url.split("/"):
                data, status_code = await request.text(configuration)
            else:
                data, status_code = await request.json(configuration)

            if data is not None:
                return data, status_code
            retry_count += 1
            await asyncio.sleep(configuration["general"]["request retry interval (sec)"])
        except aiohttp.InvalidURL:
            logging.getLogger("app").warning(
                f"api.py - {request_url} returned \"{status_code}\" ({retry_count}/{configuration['general']['request retry (count)']})"
            )
            return None, status_code
        except aiohttp.ClientError:
            retry_count += 1
            logging.getLogger("app").warning(
                f"api.py - {request_url} returned \"{status_code}\" ({retry_count}/{configuration['general']['request retry (count)']})"
            )
            await asyncio.sleep(configuration["general"]["request retry interval (sec)"])
    return None, status_code


async def get_user_ids(session, layer, requester, _configuration):
    while True:
        try:
            request_url = f"http://127.0.0.1:8000/user/ids/contact/{requester}/layer/{layer}" if requester else f"http://127.0.0.1:8000/user/ids/layer/{layer}"
            data, resp_status = await safe_request(session, request_url, _configuration)
        except asyncio.TimeoutError:
            logging.getLogger("app").error(f"api.py - localhost error:\n\t{traceback.format_exc()}")
            await asyncio.sleep(1)
        else:
            if resp_status == 200:
                return data
            logging.getLogger("app").warning(f"api.py - localhost error: {request_url} returned status {resp_status}")
            await asyncio.sleep(3)


async def locate_node(session, _configuration, requester, id_, ip, port):
    while True:
        try:
            request_url = f"http://127.0.0.1:8000/user/ids/{id_}/{ip}/{port}"
            data, resp_status = await safe_request(session, request_url, _configuration)
        except asyncio.TimeoutError:
            logging.getLogger("app").warning(f"api.py - localhost error:\n\t{traceback.format_exc()}")
            await asyncio.sleep(1)
        else:
            if resp_status == 200:
                return data
            logging.getLogger("app").warning(f"api.py - localhost error: {request_url} returned status {resp_status}")
            await asyncio.sleep(6)
