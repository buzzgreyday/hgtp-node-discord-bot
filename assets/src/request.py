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
    retry_count = 0
    status_code = None
    while True:
        try:
            data, status_code = await _conditional_safe_requests(session, request_url, cluster, configuration)
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
    _type = None
    while True:
        try:
            if requester is None:
                _type = f"automatic check, l{layer}"
                data, resp_status = await Request(
                    session, f"http://127.0.0.1:8000/user/ids/layer/{layer}"
                ).db_json(timeout=30)
            else:
                _type = f"request report ({requester}, l{layer})"
                data, resp_status = await Request(
                    session,
                    f"http://127.0.0.1:8000/user/ids/contact/{requester}/layer/{layer}",
                ).db_json(timeout=30)
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

async def node_data(node_data: schemas.Node, _configuration, requester: str | None = None):
    """Get historic node data"""

    #
    # This here is the biggest problem it seems
    # The problem seems to be with requesting sqlite3 async from different tasks. It locks or times out.
    localhost_error_retry = 0
    data = None
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                data, resp_status = await request.Request(
                    session,
                    f"http://127.0.0.1:8000/data/node/{node_data.ip}/{node_data.public_port}",
                ).db_json(timeout=6)
                # resp_status = 500
            except (
                asyncio.exceptions.TimeoutError,
                client_exceptions.ClientOSError,
                client_exceptions.ServerDisconnectedError,
            ):
                logging.getLogger("app").debug(
                    f"history.py - node_data\n"
                    f"Retry: {localhost_error_retry}/{2}\n"
                    f"Warning: data/node/{node_data.ip}/{node_data.public_port} ({localhost_error_retry}/{2}): {traceback.format_exc()}"
                )

                if localhost_error_retry <= 2:
                    localhost_error_retry += 1
                    await asyncio.sleep(1)
                else:
                    break
            else:
                # This section won't do. We need to get the historic data; the first lines won't work we need loop to ensure we get the data, only if it doesn't exist, we can continue.
                if resp_status == 200:
                    break
                else:
                    logging.getLogger("app").debug(
                        f"history.py - node_data\n"
                        f"Retry: {localhost_error_retry}/{2}\n"
                        f"Status {resp_status}"
                    )
                    if localhost_error_retry <= 2:
                        localhost_error_retry += 1
                        await asyncio.sleep(1)
                    else:
                        # Did the user unsubscribe
                        break
    if data:
        if requester:
            node_data = node_data.model_validate(data)
        else:
            data = schemas.Node(**data)
            node_data.former_node_cluster_session = data.node_cluster_session
            node_data.former_cluster_name = data.cluster_name
            node_data.last_known_cluster_name = data.last_known_cluster_name
            node_data.former_reward_state = data.reward_state
            node_data.former_cluster_connectivity = data.cluster_connectivity
            node_data.former_node_cluster_session = data.node_cluster_session
            node_data.former_cluster_association_time = data.cluster_association_time
            node_data.former_cluster_dissociation_time = data.cluster_dissociation_time
            node_data.former_timestamp_index = data.timestamp_index
            node_data.last_notified_timestamp = data.last_notified_timestamp
            node_data.former_cluster_peer_count = data.cluster_peer_count
            node_data.former_state = data.state
            # This used to only if state is offline
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


async def locate_node(node_id: str, ip: str, port: str | int):
    """Locate every subscription where ID is id_
    return await dask_client.compute(subscriber_dataframe[subscriber_dataframe.id == id_])
    """
    retry = 0
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                data, resp_status = await Request(
                    session, f"http://127.0.0.1:8000/user/ids/{node_id}/{ip}/{port}"
                ).db_json(timeout=30)
            except (
                asyncio.exceptions.TimeoutError,
                aiohttp.client_exceptions.ClientConnectorError,
                aiohttp.client_exceptions.ClientOSError,
                aiohttp.client_exceptions.ServerDisconnectedError,
                aiohttp.client_exceptions.ClientPayloadError,
            ):
                logging.getLogger("app").warning(
                    f"api.py - locate_node\n "
                    f"Retry: {retry}/2\n"
                    f"Warning: {traceback.format_exc()}"
                )
                if retry <= 2:
                    # Did the user unsubscribe?
                    logging.getLogger("app").debug(
                        f"api.py - locate_node\n"
                        f"Retry: {retry}/2\n"
                        f"Note: Did the user unsubscribe?"
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
                        f"Retry: {retry}/2\n"
                        f"Note: Did the user unsubscribe?"
                        f"Status: {resp_status}"
                    )
                    if retry <= 2:
                        retry += 1
                        await asyncio.sleep(1)
                    else:
                        break
