import asyncio
import logging
import sqlite3
from typing import List

import sqlalchemy.exc

from assets.src import schemas, api, database


async def node_data(requester, node_data: schemas.Node, _configuration):
    """Get historic node data"""

    #
    # This here is the biggest problem it seems
    # The problem seems to be with requesting sqlite3 async from different tasks. It locks or times out.
    while True:
        try:
            data, resp_status = await api.Request(f"http://127.0.0.1:8000/data/node/{node_data.ip}/{node_data.public_port}").db_json(_configuration)
        except asyncio.TimeoutError:
            logging.getLogger(__name__).warning(
                f"history.py - localhost error: data/node/{node_data.ip}/{node_data.public_port} timeout")

            await asyncio.sleep(0)
        else:
        # This section won't do. We need to get the historic data; the first lines won't work we need loop to ensure we get the data, only if it doesn't exist, we can continue.
            if resp_status == 200:
                break
            else:
                logging.getLogger(__name__).warning(f"history.py - localhost error: data/node/{node_data.ip}/{node_data.public_port} returned status {resp_status}")
                await asyncio.sleep(0)

    if data is not None:
        if requester:
            node_data = schemas.Node(**data)
        else:
            data = schemas.Node(**data)
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
            node_data.former_cluster_state = data.state
            # This used to only if state is offline
            node_data.id = data.id
            node_data.wallet_address = data.wallet_address
            node_data.version = data.version
            node_data.cpu_count = data.cpu_count
            node_data.disk_space_total = data.disk_space_total
            node_data.disk_space_free = data.disk_space_free
    return node_data
    # return pd.DataFrame([data]) if data is not None else data


async def write(data: List[schemas.Node]):
    """Write user/subscriber node data from automatic check to database"""
    if data:
        for d in data:
            while True:
                try:
                    await database.post_data(data=d)
                    break
                except sqlalchemy.exc.IntegrityError:
                    await asyncio.sleep(0)
                except (sqlite3.OperationalError, asyncio.TimeoutError):
                    await asyncio.sleep(1)

