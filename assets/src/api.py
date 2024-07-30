import asyncio
import traceback
from typing import List

import aiohttp
from aiohttp import client_exceptions
import logging

from assets.src import schemas


class Request:
    def __init__(self, session, url):
        self.url = url
        self.session = session

    async def json(self, configuration: dict):
        async with self.session.get(
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

    async def db_json(self, timeout=18000):
        async with self.session.get(self.url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
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
        async with self.session.get(self.url, timeout=timeout) as resp:
            await asyncio.sleep(0)
            if resp.status == 200:
                text = await resp.text()
                obj = schemas.NodeMetrics.from_txt(text)
                del text
                return obj, resp.status
            else:
                return None, resp.status


async def safe_request(session, request_url: str, configuration: dict):
    retry_count = 0
    status_code = None
    while True:
        try:
            if "metrics" in request_url.split("/"):
                data, status_code = await Request(session, request_url).text(
                    configuration
                )
            else:
                data, status_code = await Request(session, request_url).json(
                    configuration
                )
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
            logging.getLogger("app").debug(
                f"api.py - safe request to {request_url}\n"
                f"Status: {status_code}\n"
                f"Retry: {retry_count}/{configuration['general']['request retry (count)']}"
            )
            if retry_count >= configuration["general"]["request retry (count)"]:
                return None, status_code
            retry_count += 1
            await asyncio.sleep(
                configuration["general"]["request retry interval (sec)"]
            )

        except (aiohttp.client_exceptions.InvalidURL,) as e:
            logging.getLogger("app").debug(
                f"api.py - safe request to {request_url}\n"
                f"Status: {status_code}\n"
                f"Retry: {retry_count}/{configuration['general']['request retry (count)']}"
            )
            return None, status_code


async def get_user_ids(session, layer, requester, _configuration) -> List:
    """RETURNS A LIST/SET OF TUPLES CONTAINING ID, IP, PORT (PER LAYER)"""
    type = None
    while True:
        try:
            if requester is None:
                type = f"automatic check, l{layer}"
                data, resp_status = await Request(
                    session, f"http://127.0.0.1:8000/user/ids/layer/{layer}"
                ).db_json(timeout=6)
            else:
                type = f"request report ({requester}, l{layer})"
                data, resp_status = await Request(
                    session,
                    f"http://127.0.0.1:8000/user/ids/contact/{requester}/layer/{layer}",
                ).db_json(timeout=6)
        except (
            asyncio.exceptions.TimeoutError,
            aiohttp.client_exceptions.ClientConnectorError,
            aiohttp.client_exceptions.ClientOSError,
            aiohttp.client_exceptions.ServerDisconnectedError,
            aiohttp.client_exceptions.ClientPayloadError,
        ):
            logging.getLogger("app").debug(
                f"api.py - get_user_ids from localhost\n"
                f"Type: {type}\n"
                f"Error: {traceback.format_exc()}"
            )
            await asyncio.sleep(1)
        else:
            if resp_status == 200:
                return data
            if resp_status == 500:
                await asyncio.sleep(3)
            else:
                logging.getLogger("app").warning(
                    f"api.py - get_user_ids\n"
                    f"Status: {resp_status}"
                )
                await asyncio.sleep(3)


async def locate_node(session, _configuration, requester, id_, ip, port):
    """Locate every subscription where ID is id_
    return await dask_client.compute(subscriber_dataframe[subscriber_dataframe.id == id_])
    """
    retry = 0
    while True:
        try:
            data, resp_status = await Request(
                session, f"http://127.0.0.1:8000/user/ids/{id_}/{ip}/{port}"
            ).db_json(timeout=6)
        except (
            asyncio.exceptions.TimeoutError,
            aiohttp.client_exceptions.ClientConnectorError,
            aiohttp.client_exceptions.ClientOSError,
            aiohttp.client_exceptions.ServerDisconnectedError,
            aiohttp.client_exceptions.ClientPayloadError,
        ):
            logging.getLogger("app").warning(
                f"api.py - locate_node\n "
                f"Retry: {retry/2}\n"
                f"Warning: {traceback.format_exc()}"
            )
            if retry <= 2:
                retry += 1
                await asyncio.sleep(1)
            else:
                break
        else:
            if resp_status == 200:
                return data
            else:
                # Did the user unsubscribe?
                logging.getLogger("app").debug(
                    f"api.py - locate_node\n"
                    f"Retry: {retry}/{2}\n"
                    f"Note: Did the user unsubscribe?"
                    f"Status: {resp_status}"
                )
                if retry <= 2:
                    retry += 1
                    await asyncio.sleep(1)
                else:
                    break
