import asyncio
import logging
import traceback
from typing import List

from aiohttp import client_exceptions

from assets.src import schemas, api, database
from assets.src.database import database


async def node_data(session, requester, node_data: schemas.Node, _configuration):
    """Get historic node data"""

    #
    # This here is the biggest problem it seems
    # The problem seems to be with requesting sqlite3 async from different tasks. It locks or times out.
    localhost_error_retry = 0
    while True:
        try:
            data, resp_status = await api.Request(
                session,
                f"http://127.0.0.1:8000/data/node/{node_data.ip}/{node_data.public_port}",
            ).db_json(_configuration)
        except (
            asyncio.exceptions.TimeoutError,
            client_exceptions.ClientOSError,
            client_exceptions.ServerDisconnectedError,
        ):
            logging.getLogger("app").warning(
                f"history.py - localhost error - status {resp_status}: data/node/{node_data.ip}/{node_data.public_port} ({localhost_error_retry}/{2}): {traceback.format_exc()}"
            )
            if localhost_error_retry <= 2:
                await asyncio.sleep(1)
            else:
                break
        else:
            # This section won't do. We need to get the historic data; the first lines won't work we need loop to ensure we get the data, only if it doesn't exist, we can continue.
            if resp_status == 200:
                break
            else:
                logging.getLogger("app").warning(
                    f"history.py - localhost error - status {resp_status}: data/node/{node_data.ip}/{node_data.public_port} ({localhost_error_retry}/{2}): {traceback.format_exc()}"
                )
                if localhost_error_retry <= 2:
                    await asyncio.sleep(1)
                else:
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


async def write(data: List[schemas.Node]):
    """Write user/subscriber node data from automatic check to database"""

    if data:
        for d in data:
            await database.post_data(data=d)
