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
    data = await api.Request(f"http://127.0.0.1:8000/data/node/{node_data.ip}/{node_data.public_port}").json(_configuration)
    return schemas.Node(former_cluster_name=data.cluster_name,
                        last_known_cluster_name=data.last_known_cluster_name,
                        former_reward_state=data.reward_state,
                        former_cluster_connectivity=data.cluster_connectivity,
                        )
    # return pd.DataFrame([data]) if data is not None else data


async def write(data: List[schemas.Node]):
    """Write user/subscriber node data from automatic check to database"""
    async with database.SessionLocal() as session:
        db = session
        for d in data:
            await database.post_data(data=d, db=db)


def merge_data(node_data: schemas.Node, cluster_data, historic_node_dataframe) -> schemas.Node:
    """Transfer historic node data to current node data object"""
    if historic_node_dataframe is not None:
        node_data.former_cluster_name = Clean(historic_node_dataframe["cluster_name"]).make_lower()
        node_data.last_known_cluster_name = Clean(historic_node_dataframe.last_known_cluster_name).make_none()
        node_data.former_reward_state = bool(Clean(historic_node_dataframe["reward_state"]).make_none())
        node_data.former_cluster_connectivity = Clean(historic_node_dataframe["cluster_connectivity"]).make_none()
        node_data.former_node_cluster_session = Clean(historic_node_dataframe["node_cluster_session"]).make_none()
        node_data.former_cluster_association_time = Clean(historic_node_dataframe["cluster_association_time"]).make_none()
        node_data.former_cluster_dissociation_time = Clean(historic_node_dataframe["cluster_dissociation_time"]).make_none()
        node_data.former_timestamp_index = datetime.strptime(Clean(historic_node_dataframe["timestamp_index"]).make_none(), "%Y-%m-%dT%H:%M:%S.%f")
        last_notified = Clean(historic_node_dataframe["last_notified_timestamp"]).make_none()
        if last_notified is not None:
            node_data.last_notified_timestamp = datetime.strptime(last_notified, "%Y-%m-%dT%H:%M:%S.%f")
        if node_data.state == "Offline":
            node_data.id = Clean(historic_node_dataframe["id"]).make_none()
            node_data.wallet_address = Clean(historic_node_dataframe["wallet_address"]).make_none()
            node_data.version = Clean(historic_node_dataframe["version"]).make_none()
            node_data.cpu_count = float(historic_node_dataframe["cpu_count"])
            node_data.disk_space_total = float(historic_node_dataframe["disk_space_total"])
            node_data.disk_space_free = float(historic_node_dataframe["disk_space_free"])
        if cluster_data is not None:
            if cluster_data["name"] == node_data.former_cluster_name:
                node_data.former_cluster_peer_count = cluster_data["peer_count"]
                node_data.former_cluster_state = cluster_data["state"]

    return node_data
