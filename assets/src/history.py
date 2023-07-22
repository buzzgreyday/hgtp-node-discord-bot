import json
from datetime import datetime
from typing import List

import pandas as pd

from assets.src import schemas, api, database


class Clean:
    def __init__(self, value):
        self._value = value

    def make_lower(self) -> str | None:
        self._value = Clean(self._value).make_none()
        if self._value is not None:
            return self._value.lower()

    def make_none(self) -> str | None:
        if len(self._value) != 0:
            return self._value.values[0]
        else:
            return None


async def node_data(node_data: schemas.Node, _configuration):
    """THIS IS NOT DONE!!!!!!!!!!!!"""
    data = schemas.Node(**await api.Request(f"http://127.0.0.1:8000/data/node/{node_data.ip}/{node_data.public_port}").json(_configuration))
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
    if node_data.state == "Offline":
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
    async with database.SessionLocal() as session:
        db = session
        for d in data:
            await database.post_data(data=d, db=db)
