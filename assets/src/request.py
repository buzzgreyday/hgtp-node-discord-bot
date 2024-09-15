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

    async def json(self, timeout: int = 12):
        async with self.session.get(
            self.url,
            timeout=aiohttp.ClientTimeout(
                total=timeout
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


async def _conditional_safe_requests(session, request_url: str, cluster: bool, configuration: dict):
    if "metrics" in request_url.split("/"):
        return await Request(session, request_url).text(
            configuration
        )
    else:
        if cluster:
            return await Request(session, request_url).json(
                int(configuration["general"]["cluster request timeout (sec)"])
            )
        else:
            return await Request(session, request_url).json(
                int(configuration["general"]["request timeout (sec)"])
            )


async def safe_request(session, request_url: str, configuration: dict, cluster=False):
    retry = 0
    status_code = None
    while retry <= configuration["general"]["request retry (count)"]:
        try:
            data, status_code = await _conditional_safe_requests(session, request_url, cluster, configuration)
            if data is not None and status_code == 200:
                return data, status_code
            else:
                retry += 1
                await asyncio.sleep(
                    configuration["general"]["request retry interval (sec)"] ** retry
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
                f"Retry: {retry}/{configuration['general']['request retry (count)']} in {configuration["general"]["request retry interval (sec)"] ** retry} seconds"
            )
            retry += 1
            await asyncio.sleep(
                configuration["general"]["request retry interval (sec)"] ** retry
            )

        except (aiohttp.client_exceptions.InvalidURL,) as e:
            logging.getLogger("app").debug(
                f"api.py - safe request to {request_url}\n"
                f"Status: {status_code}\n"
                f"Retry: {retry}/{configuration['general']['request retry (count)']}"
            )
            break
    return None, status_code


async def get_user_ids(session, layer, requester: str | None = None, retry: int = 1, max_retries: int = 3, sleep: int = 1, timeout: int = 60) -> List:
    """RETURNS A LIST/SET OF TUPLES CONTAINING ID, IP, PORT (PER LAYER)"""
    _type = None
    while retry <= max_retries:
        try:
            if requester is None:
                _type = f"automatic check, l{layer}"
                data, resp_status = await Request(
                    session, f"http://127.0.0.1:8000/user/ids/layer/{layer}"
                ).db_json(timeout=timeout)
            else:
                _type = f"request report ({requester}, l{layer})"
                data, resp_status = await Request(
                    session,
                    f"http://127.0.0.1:8000/user/ids/contact/{requester}/layer/{layer}",
                ).db_json(timeout=timeout)
        except (
            asyncio.exceptions.TimeoutError,
            aiohttp.client_exceptions.ClientConnectorError,
            aiohttp.client_exceptions.ClientOSError,
            aiohttp.client_exceptions.ServerDisconnectedError,
            aiohttp.client_exceptions.ClientPayloadError,
        ):
            logging.getLogger("app").debug(
                f"request.py - get_user_ids from localhost\n"
                f"Type: {_type}\n"
                f"Error: {traceback.format_exc()}"
            )
            await asyncio.sleep(sleep ** retry)
        else:
            if resp_status == 200:
                return data
            else:
                logging.getLogger("app").warning(
                    f"request.py - get_user_ids\n"
                    f"Response status: {resp_status}"
                )
                await asyncio.sleep(sleep ** retry)
    return []


async def node_data(node_data: schemas.Node, requester: str | None = None, retry: int = 1, max_retries: int = 3, sleep: int = 1, timeout: int = 60):
    """Get historic node data"""

    data = None

    async with aiohttp.ClientSession() as session:
        while retry <= max_retries:
            try:
                data, resp_status = await Request(
                    session,
                    f"http://127.0.0.1:8000/data/node/{node_data.ip}/{node_data.public_port}",
                ).db_json(timeout=timeout)
            except (
                    asyncio.exceptions.TimeoutError,
                    client_exceptions.ClientOSError,
                    client_exceptions.ServerDisconnectedError,
            ):
                logging.getLogger("app").warning(
                    f"request.py - node_data\n"
                    f"Retry: {retry}/{max_retries} in {sleep ** retry} seconds\n"
                    f"Endpoint: data/node/{node_data.ip}/{node_data.public_port}\n"
                    f"Details: {traceback.format_exc()}"
                )
                retry += 1
                await asyncio.sleep(sleep ** retry)
            except Exception:
                logging.getLogger("app").critical(
                    f"request.py - node_data\n"
                    f"Retry: {retry}/{max_retries} in {sleep ** retry} seconds\n"
                    f"Endpoint: data/node/{node_data.ip}/{node_data.public_port}\n"
                    f"Details: {traceback.format_exc()}"
                )
                retry += 1
                await asyncio.sleep(sleep ** retry)
            else:
                if resp_status == 200:
                    break
                else:
                    logging.getLogger("app").warning(
                        f"history.py - node_data\n"
                        f"Retry: {retry}/{max_retries} in {sleep ** retry} seconds\n"
                        f"Response status {resp_status}"
                    )
                    retry += 1
                    await asyncio.sleep(sleep)

    if data:
        # Populate node_data
        if requester:
            node_data = node_data.model_validate(data)
        else:
            data = schemas.Node(**data)
            node_data.former_node_cluster_session = data.node_cluster_session
            node_data.former_cluster_name = data.cluster_name
            node_data.last_known_cluster_name = data.last_known_cluster_name
            node_data.former_reward_state = data.reward_state
            node_data.former_cluster_connectivity = data.cluster_connectivity
            node_data.former_cluster_association_time = data.cluster_association_time
            node_data.former_cluster_dissociation_time = data.cluster_dissociation_time
            node_data.former_timestamp_index = data.timestamp_index
            node_data.last_notified_timestamp = data.last_notified_timestamp
            node_data.former_cluster_peer_count = data.cluster_peer_count
            node_data.former_state = data.state
            node_data.id = data.id
            node_data.wallet_address = data.wallet_address
            node_data.version = data.version
            if not node_data.cpu_count:
                node_data.cpu_count = data.cpu_count
                node_data.disk_space_total = data.disk_space_total
                node_data.disk_space_free = data.disk_space_free
            if not node_data.wallet_balance:
                node_data.wallet_balance = data.wallet_balance

    return node_data


async def locate_node(node_id: str, ip: str, port: str | int, retry: int = 1, max_retries: int = 3, sleep: int = 1, timeout: int = 60):
    """Locate every subscription where ID is id_
    return await dask_client.compute(subscriber_dataframe[subscriber_dataframe.id == id_])
    """

    async with aiohttp.ClientSession() as session:
        while retry <= max_retries:
            try:
                data, resp_status = await Request(
                    session, f"http://127.0.0.1:8000/user/ids/{node_id}/{ip}/{port}"
                ).db_json(timeout=timeout)
            except (
                asyncio.exceptions.TimeoutError,
                aiohttp.client_exceptions.ClientConnectorError,
                aiohttp.client_exceptions.ClientOSError,
                aiohttp.client_exceptions.ServerDisconnectedError,
                aiohttp.client_exceptions.ClientPayloadError,
            ):

                # Did the user unsubscribe?
                logging.getLogger("app").warning(
                    f"api.py - locate_node\n "
                    f"Retry: {retry}/{max_retries}\n"
                    f"Endpoint: user/ids/{node_id}/{ip}/{port}\n"
                    f"Details: {traceback.format_exc()}"
                )
                retry += 1
                await asyncio.sleep(sleep ** retry)
            except Exception:
                # Did the user unsubscribe?
                logging.getLogger("app").critical(
                    f"api.py - locate_node\n "
                    f"Retry: {retry}/{max_retries}\n"
                    f"Endpoint: user/ids/{node_id}/{ip}/{port}\n"
                    f"Details: {traceback.format_exc()}"
                )
                retry += 1
                await asyncio.sleep(sleep ** retry)
            else:
                if resp_status == 200:
                    return data
                else:
                    # Did the user unsubscribe?

                    logging.getLogger("app").debug(
                        f"api.py - locate_node\n"
                        f"Retry: {retry}/{max_retries} in {sleep ** retry} seconds\n"
                        f"Response status: {resp_status}"
                    )
                    retry += 1
                    await asyncio.sleep(sleep ** retry)

    return
